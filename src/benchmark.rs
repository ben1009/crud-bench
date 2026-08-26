//! Orchestrates benchmark phases (CRUD, scans, batches) against a [`crate::engine::BenchmarkEngine`].
//!
//! Spawns concurrent clients/threads, records latency histograms, and aggregates
//! [`crate::result::OperationResult`] values for reporting.

use crate::dialect::Dialect;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::keyprovider::KeyProvider;
use crate::result::{
	BenchmarkMetadata, BenchmarkResult, OperationMetric, OperationResult, ScanResult, ScanRun,
	ScanWorkload, SteadyStateDrain, SteadyStateLatency, SteadyStatePhase, SteadyStatePhases,
	SteadyStateResult, SteadyStateStatus, SteadyStateTask, SteadyStateThroughput,
	SteadyStateValidation, writes_ratio_percent,
};
use crate::system::SystemInfo;
use crate::terminal::BenchUi;
use crate::util::format_duration;
use crate::value::BenchValue;
use crate::valueprovider::ColumnType;
use crate::valueprovider::ValueProvider;
use crate::workloads;
use crate::{
	Args, BatchOperation, Batches, Index, Scan, ScanWithWrites, Scans, SteadyStatePreset, Suite,
	VectorHoldout, VectorIndexStrategy, VectorQuerySpec,
};

use anyhow::{Context, Result, bail};
use futures::future::try_join_all;
use hdrhistogram::Histogram;
use indicatif::ProgressBar;
use log::{debug, info};
use tokio::task::JoinSet;
use tokio::time::Instant;

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// Maximum wait when polling until the first datastore client connects.
const TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_STEADY_STATE_WORKLOADS: &[SteadyStateWorkload] = &[
	SteadyStateWorkload::BalancedZipfian,
	SteadyStateWorkload::ReadHeavyZipfian,
	SteadyStateWorkload::UpdateHeavyZipfian,
	SteadyStateWorkload::PointReadZipfian,
	SteadyStateWorkload::PointReadUniform,
	SteadyStateWorkload::PointReadMissingInRange,
	SteadyStateWorkload::RangeScanUniform,
	SteadyStateWorkload::SustainedIngest,
];
const MAX_OPERATION_MIX_PERIOD: u32 = 100_000;

/// Fixed sleep between phases to let any server-side phase tail settle
/// (open snapshots, draining tasks) before the next phase opens its
/// profiling window. Conservative — short enough to be invisible to a
/// human, long enough to mop up the kind of MVCC drain visible in
/// SurrealDB/RocksDB after heavy concurrent scans.
const QUIESCE_DELAY: Duration = Duration::from_secs(1);

/// Error string returned by adapters to mark an operation as unsupported (skipped, not fatal).
pub(crate) const NOT_SUPPORTED_ERROR: &str = "NotSupported";

/// Pre-fetched query set for a vector-search scan. Holds `count` query vectors
/// sampled deterministically from the inserted records, indexed by sample
/// number with simple modulo wrap-around. Memory cost is constant in `count`
/// (independent of the dataset size).
#[derive(Debug, Clone)]
pub(crate) struct VectorQuerySet {
	pub(crate) queries: Arc<Vec<Vec<f32>>>,
}

impl VectorQuerySet {
	pub(crate) fn pick(&self, sample: u32) -> &[f32] {
		let q = &self.queries[(sample as usize) % self.queries.len()];
		q.as_slice()
	}
}

/// Pull a `FloatVector` out of a row's named column, accepting the packed
/// [`BenchValue::FloatVector`], a generic `Array<Float>` (SurrealDB), or a
/// `Bytes` payload of packed little-endian f32s (SQL backends that fall back
/// to BYTEA/BLOB columns for vectors).
///
/// Any other shape — including a missing field — bails with
/// [`NOT_SUPPORTED_ERROR`] so the scan skips cleanly on backends that don't
/// round-trip vector payloads in any of these forms.
fn extract_vector_field(row: &BenchValue, field: &str) -> Result<Vec<f32>> {
	let Some(v) = row.get_field(field) else {
		bail!(NOT_SUPPORTED_ERROR);
	};
	match v {
		BenchValue::FloatVector(v) => Ok(v.clone()),
		BenchValue::Array(a) => {
			let mut out = Vec::with_capacity(a.len());
			for elem in a {
				match elem {
					BenchValue::Float(f) => out.push(*f as f32),
					BenchValue::Int(i) => out.push(*i as f32),
					BenchValue::UInt(u) => out.push(*u as f32),
					BenchValue::Decimal(d) => {
						out.push(rust_decimal::prelude::ToPrimitive::to_f32(d).unwrap_or(0.0))
					}
					_ => bail!(NOT_SUPPORTED_ERROR),
				}
			}
			Ok(out)
		}
		BenchValue::Bytes(b) if b.len() % 4 == 0 => Ok(bytemuck::cast_slice::<u8, f32>(b).to_vec()),
		_ => bail!(NOT_SUPPORTED_ERROR),
	}
}

/// Deterministically pick `count` sample indices from `[0, samples)` using `seed`.
fn holdout_indices(samples: u32, count: usize, seed: u64) -> Vec<u32> {
	use rand::RngExt as _;
	use rand::SeedableRng;
	use rand::prelude::SmallRng;
	let total = samples as usize;
	let count = count.min(total);
	let mut rng = SmallRng::seed_from_u64(seed);
	let mut out = Vec::with_capacity(count);
	for _ in 0..count {
		let pick = rng.random_range(0u32..samples);
		out.push(pick);
	}
	out
}

/// Shared benchmark settings and UI, built from CLI [`crate::Args`].
pub(crate) struct Benchmark {
	/// Whether to run containers in privileged mode
	pub(crate) privileged: bool,
	/// The container image to use
	pub(crate) image: Option<String>,
	/// Whether to skip the delete phase
	pub(crate) skip_deletes: bool,
	/// Whether to skip all write phases (create, update, delete, batch writes)
	pub(crate) skip_writes: bool,
	/// The server endpoint to connect to
	pub(crate) endpoint: Option<String>,
	/// The number of clients to spawn
	pub(crate) clients: u32,
	/// The number of threads to spawn
	pub(crate) threads: u32,
	/// The number of samples to run
	pub(crate) samples: u32,
	/// Pid to monitor
	pub(crate) pid: Option<u32>,
	/// Whether to ensure data is synced
	pub(crate) sync: bool,
	/// Whether to enable disk persistence
	pub(crate) persisted: bool,
	/// Whether to enable optimised configurations
	pub(crate) optimised: bool,
	/// Per-operation timeout
	pub(crate) operation_timeout: Duration,
	/// Terminal UI (tables, progress bars, phase markers).
	pub(crate) bench_ui: BenchUi,
	/// Grep-friendly `… starting` / `Benchmark starting` lines for profiling scripts
	pub(crate) emit_phase_markers: bool,
	pub(crate) suite: Suite,
	pub(crate) steady_state_benches: Option<String>,
	pub(crate) steady_state_preset: SteadyStatePreset,
	pub(crate) warmup_secs: Option<u64>,
	pub(crate) measurement_secs: Option<u64>,
	pub(crate) latency_sample_every: Option<u64>,
	pub(crate) seed: u64,
	pub(crate) zipfian_exponent: f64,
	pub(crate) operation_mix: Option<String>,
	pub(crate) operation_mix_period: u32,
}

impl Benchmark {
	/// Builds runtime settings from parsed CLI arguments (including env-driven phase markers).
	pub(crate) fn new(args: &Args) -> Self {
		let emit_phase_markers = args.emit_phase_markers
			|| matches!(
				std::env::var("CRUD_BENCH_EMIT_PHASE_MARKERS").as_deref(),
				Ok("1" | "true" | "yes" | "on")
			);
		Self {
			privileged: args.privileged,
			image: args.image.to_owned(),
			endpoint: args.endpoint.to_owned(),
			clients: args.clients,
			threads: args.threads,
			samples: args.samples,
			sync: args.sync,
			pid: args.pid,
			persisted: args.persisted,
			optimised: args.optimised,
			skip_deletes: args.skip_deletes,
			skip_writes: args.skip_writes,
			operation_timeout: Duration::from_secs(args.operation_timeout),
			bench_ui: BenchUi::new(args.color),
			emit_phase_markers,
			suite: args.suite,
			steady_state_benches: args.bench.clone(),
			steady_state_preset: args.preset,
			warmup_secs: args.warmup_secs,
			measurement_secs: args.measurement_secs,
			latency_sample_every: args.latency_sample_every,
			seed: args.seed,
			zipfian_exponent: args.zipfian_exponent,
			operation_mix: args.operation_mix.clone(),
			operation_mix_period: args.operation_mix_period,
		}
	}

	/// When `COMPACTION` is set in the environment, run the engine-specific
	/// compaction hook and print elapsed time (same style as phase lines).
	async fn maybe_compact_datastore<C, E>(&self, engine: &E) -> Result<()>
	where
		C: BenchmarkClient + Send + Sync,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		if std::env::var("COMPACTION").is_ok() {
			if self.emit_phase_markers {
				self.bench_ui.println_plain("Compaction starting");
			}
			let t = Instant::now();
			self.wait_for_client(engine).await?.compact().await?;
			self.bench_ui.println_took_head("Compaction", &format_duration(t.elapsed()));
			self.quiesce_and_mark().await;
		}
		Ok(())
	}

	/// Sleep a fixed beat to let any server-side phase tail settle (open
	/// snapshots, draining tasks, deferred cleanup that outlives the
	/// client's `try_join_all`), then emit the grep-friendly `Server idle`
	/// marker. dev.sh uses that line to disable + rotate the active perf
	/// window so each phase's flamegraph excludes the next phase's startup
	/// work *and* includes its own server-side tail.
	///
	/// Plain sleep — no client probe — so the marker can't silently wedge
	/// the benchmark if a probe query gets stuck.
	async fn quiesce_and_mark(&self) {
		tokio::time::sleep(QUIESCE_DELAY).await;
		if self.emit_phase_markers {
			self.bench_ui.println_plain("Server idle");
		}
	}

	#[allow(clippy::too_many_arguments)]
	/// Run the benchmark for the desired benchmark engine
	pub(crate) async fn run<C, D, E>(
		&self,
		engine: E,
		kp: KeyProvider,
		mut vp: ValueProvider,
		scans: Scans,
		batches: Batches,
		database: Option<String>,
		system: Option<SystemInfo>,
		metadata: Option<BenchmarkMetadata>,
	) -> Result<BenchmarkResult>
	where
		C: BenchmarkClient + Send + Sync,
		D: Dialect,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		// Generate a value sample for the report
		let sample = vp.generate_value();
		// Setup the datastore
		self.bench_ui
			.println_muted(&format!("Setting up the datastore with {} clients", self.clients));
		// Setup the datastore
		self.wait_for_client(&engine).await?.startup().await?;
		// Setup the clients
		let clients = self.setup_clients(&engine).await?;
		// Start the benchmark (optional line for log-based profiling)
		if self.emit_phase_markers {
			self.bench_ui.println_plain("Benchmark starting");
		}
		if self.suite == Suite::SteadyState {
			let steady_state =
				self.run_steady_state::<C>(&clients, kp, vp.clone(), database.clone()).await?;
			if self.emit_phase_markers {
				self.bench_ui.println_plain("Benchmark complete");
			}
			self.wait_for_client(&engine).await?.shutdown().await?;
			return Ok(BenchmarkResult {
				database,
				system,
				metadata,
				creates: None,
				reads: None,
				updates: None,
				scans: Vec::new(),
				steady_state,
				batches: Vec::new(),
				deletes: None,
				sample,
			});
		}
		// Run the "creates" benchmark (skipped if --skip-writes)
		let creates = if self.skip_writes {
			None
		} else {
			self.run_operation::<C, D>(
				&clients,
				BenchmarkOperation::Create,
				kp,
				vp.clone(),
				self.samples,
			)
			.await?
		};
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		// Run the "reads" benchmark
		let reads = self
			.run_operation::<C, D>(&clients, BenchmarkOperation::Read, kp, vp.clone(), self.samples)
			.await?;
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		// Run the "updates" benchmark (skipped if --skip-writes)
		let updates = if self.skip_writes {
			None
		} else {
			self.run_operation::<C, D>(
				&clients,
				BenchmarkOperation::Update,
				kp,
				vp.clone(),
				self.samples,
			)
			.await?
		};
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		// Run the "scan" benchmarks
		let mut scan_results = Vec::with_capacity(scans.len());
		let mut prev_spec_group: Option<u32> = None;
		let mut prev_run_key: Option<(u32, String)> = None;
		for scan in scans {
			// New section in the TOML/config → new heading in the CLI output
			if prev_spec_group != Some(scan.spec_group) {
				self.bench_ui.section_header(&format!("Scan · {}", scan.id));
				prev_spec_group = Some(scan.spec_group);
			}
			// Multi-run entries (`runs` array): print a sub-line when the run name changes
			let run_key = (scan.spec_group, scan.name.clone());
			if scan.multi_run_spec && prev_run_key.as_ref() != Some(&run_key) {
				self.bench_ui.println_scan_run(&scan.name);
				prev_run_key = Some(run_key);
			} else if !scan.multi_run_spec {
				prev_run_key = Some(run_key);
			}
			let id = scan.id.clone();
			let name = scan.name.clone();
			let iterations = scan.iterations.map(|s| s as u32).unwrap_or(self.samples);
			let write_specs = scan.with_writes.as_slice();
			let w = write_specs.len();
			let index_spec = scan.with_index.as_ref().filter(|i| !i.skip);

			// Vector-search scans take a dedicated path. Order matters:
			//   1. Build the holdout query set (skipped if the engine can't
			//      surface a readable vector — same skip semantics as fulltext).
			//   2. Always invoke BuildVectorIndex. Engines decide whether the
			//      chosen strategy needs an actual index (Redis Bruteforce
			//      builds a FLAT FT index; Surreal/Postgres Bruteforce return
			//      NotSupported and the scan still runs without one).
			//   3. Run the timed VectorScan.
			//   4. RemoveIndex iff Build succeeded — strictly after the scan.
			let result = if let Some(vq) = scan.vector_query.clone() {
				let dim = vp
					.columns()
					.0
					.iter()
					.find_map(|(n, t)| match t {
						ColumnType::FloatVector(d) if n == &vq.field => Some(*d),
						_ => None,
					})
					.ok_or_else(|| {
						anyhow::anyhow!(
							"scan `{}`: vector_query.field `{}` must be a `vector:<dim>` column in the schema",
							name,
							vq.field
						)
					})?;
				let strategy_needs_index = matches!(
					vq.index_strategy,
					VectorIndexStrategy::Hnsw { .. } | VectorIndexStrategy::DiskAnn { .. }
				);
				let query_set = self
					.build_vector_query_set::<C>(&clients[0], &scan, &vq, kp, self.samples)
					.await?;
				let mut runs = Vec::with_capacity(1);
				match query_set {
					None => {
						// Engine doesn't surface vector reads — skip the whole scan.
						runs.push(ScanRun {
							workload: ScanWorkload::Read,
							indexed: strategy_needs_index,
							result: None,
						});
						ScanResult {
							id: id.clone(),
							name,
							iterations,
							index_build: None,
							index_remove: None,
							runs,
						}
					}
					Some(query_set) => {
						// Derive the index spec from `vector_query.field` so
						// the user only declares the field once. Engines that
						// don't need an index for the chosen strategy ignore
						// `idx_spec` and return NotSupported from build.
						let idx_spec = Index {
							skip: false,
							fields: vec![vq.field.clone()],
							unique: None,
							index_type: None,
						};
						let vec_index_build = self
							.run_operation::<C, D>(
								&clients[..1],
								BenchmarkOperation::BuildVectorIndex(
									idx_spec,
									vq.clone(),
									dim,
									id.clone(),
								),
								kp,
								vp.clone(),
								1,
							)
							.await?;
						if vec_index_build.is_some() {
							self.maybe_compact_datastore::<C, E>(&engine).await?;
						}
						// Run the scan if either the strategy doesn't require
						// an index (so a missing build is fine) or build
						// actually produced an index. HNSW/DiskANN with no
						// index = skip.
						let ctx = if strategy_needs_index {
							ScanContext::WithIndex
						} else {
							ScanContext::WithoutIndex
						};
						let scan_result = if !strategy_needs_index || vec_index_build.is_some() {
							self.run_operation::<C, D>(
								&clients,
								BenchmarkOperation::VectorScan(
									scan.clone(),
									ctx,
									query_set.clone(),
								),
								kp,
								vp.clone(),
								iterations,
							)
							.await?
						} else {
							None
						};
						// Drop the index *after* the scan finishes — strictly
						// in this order so the timed scan sees the index.
						let vec_index_remove = if vec_index_build.is_some() {
							self.run_operation::<C, D>(
								&clients[..1],
								BenchmarkOperation::RemoveIndex(id.clone(), name.clone()),
								kp,
								vp.clone(),
								1,
							)
							.await?
						} else {
							None
						};
						runs.push(ScanRun {
							workload: ScanWorkload::Read,
							indexed: strategy_needs_index,
							result: scan_result,
						});
						ScanResult {
							id: id.clone(),
							name,
							iterations,
							index_build: vec_index_build,
							index_remove: vec_index_remove,
							runs,
						}
					}
				}
			} else if let Some(index_spec) = index_spec {
				// Indexed scan: heap legs → build index → indexed legs → drop index
				let mut runs = Vec::with_capacity(2 + 2 * w);
				// Table-scan / heap query (no physical index)
				let without_index = self
					.run_operation::<C, D>(
						&clients,
						BenchmarkOperation::Scan(scan.clone(), ScanContext::WithoutIndex),
						kp,
						vp.clone(),
						iterations,
					)
					.await?;
				runs.push(ScanRun {
					workload: ScanWorkload::Read,
					indexed: false,
					result: without_index,
				});
				// Optional mixed read+write legs on the heap path (one per `with_writes` entry)
				for spec in write_specs {
					let mixed_without_index = if self.skip_writes {
						None
					} else {
						self.run_operation::<C, D>(
							&clients,
							BenchmarkOperation::ScanWithWrites(
								scan.clone(),
								ScanContext::WithoutIndex,
								spec.clone(),
							),
							kp,
							vp.clone(),
							iterations,
						)
						.await?
					};
					runs.push(ScanRun {
						workload: ScanWorkload::ReadWrite {
							write_ratio_percent: writes_ratio_percent(spec),
						},
						indexed: false,
						result: mixed_without_index,
					});
				}
				// BuildIndex uses a single client to avoid races on DDL
				let index_build = self
					.run_operation::<C, D>(
						&clients[..1],
						BenchmarkOperation::BuildIndex(
							index_spec.clone(),
							id.clone(),
							name.clone(),
						),
						kp,
						vp.clone(),
						1,
					)
					.await?;
				let (with_index, index_remove, indexed_write_results) = if index_build.is_some() {
					// Compact the datastore so the indexed-scan phases benchmark a compacted index.
					self.maybe_compact_datastore::<C, E>(&engine).await?;
					// Same query shape using the new index
					let with_index = self
						.run_operation::<C, D>(
							&clients,
							BenchmarkOperation::Scan(scan.clone(), ScanContext::WithIndex),
							kp,
							vp.clone(),
							iterations,
						)
						.await?;
					let mut iw = Vec::with_capacity(w);
					for spec in write_specs {
						let result = if self.skip_writes {
							None
						} else {
							self.run_operation::<C, D>(
								&clients,
								BenchmarkOperation::ScanWithWrites(
									scan.clone(),
									ScanContext::WithIndex,
									spec.clone(),
								),
								kp,
								vp.clone(),
								iterations,
							)
							.await?
						};
						iw.push(result);
					}
					let index_remove = self
						.run_operation::<C, D>(
							&clients[..1],
							BenchmarkOperation::RemoveIndex(id.clone(), name.clone()),
							kp,
							vp.clone(),
							1,
						)
						.await?;
					(with_index, index_remove, iw)
				} else {
					// BuildIndex unsupported or skipped → no indexed timings to merge
					(None, None, Vec::new())
				};
				if index_build.is_some() {
					runs.push(ScanRun {
						workload: ScanWorkload::Read,
						indexed: true,
						result: with_index,
					});
					for (spec, r) in write_specs.iter().zip(indexed_write_results) {
						runs.push(ScanRun {
							workload: ScanWorkload::ReadWrite {
								write_ratio_percent: writes_ratio_percent(spec),
							},
							indexed: true,
							result: r,
						});
					}
				} else {
					// Still emit indexed rows so CSV/HTML rows align; cells show "-" when result is None
					runs.push(ScanRun {
						workload: ScanWorkload::Read,
						indexed: true,
						result: None,
					});
					for spec in write_specs {
						runs.push(ScanRun {
							workload: ScanWorkload::ReadWrite {
								write_ratio_percent: writes_ratio_percent(spec),
							},
							indexed: true,
							result: None,
						});
					}
				}
				ScanResult {
					id: id.clone(),
					name,
					iterations,
					index_build,
					index_remove,
					runs,
				}
			} else {
				// No index spec (or index skipped): only heap scan + optional write-mix legs
				let mut runs = Vec::with_capacity(1 + w);
				let without_index = self
					.run_operation::<C, D>(
						&clients,
						BenchmarkOperation::Scan(scan.clone(), ScanContext::WithoutIndex),
						kp,
						vp.clone(),
						iterations,
					)
					.await?;
				runs.push(ScanRun {
					workload: ScanWorkload::Read,
					indexed: false,
					result: without_index,
				});
				for spec in write_specs {
					let mixed_without_index = if self.skip_writes {
						None
					} else {
						self.run_operation::<C, D>(
							&clients,
							BenchmarkOperation::ScanWithWrites(
								scan.clone(),
								ScanContext::WithoutIndex,
								spec.clone(),
							),
							kp,
							vp.clone(),
							iterations,
						)
						.await?
					};
					runs.push(ScanRun {
						workload: ScanWorkload::ReadWrite {
							write_ratio_percent: writes_ratio_percent(spec),
						},
						indexed: false,
						result: mixed_without_index,
					});
				}
				ScanResult {
					id: id.clone(),
					name,
					iterations,
					index_build: None,
					index_remove: None,
					runs,
				}
			};
			scan_results.push(result);
		}
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		// Run the "deletes" benchmark (skipped if --skip-deletes or --skip-writes)
		let deletes = if self.skip_deletes || self.skip_writes {
			self.bench_ui.section_header("Delete (skipped)");
			None
		} else {
			self.bench_ui.section_header("Delete");
			self.run_operation::<C, D>(
				&clients,
				BenchmarkOperation::Delete,
				kp,
				vp.clone(),
				self.samples,
			)
			.await?
		};
		// Compact the datastore
		self.maybe_compact_datastore::<C, E>(&engine).await?;
		if !batches.is_empty() {
			self.bench_ui.section_header("Batches");
		}
		// Run the "batch" benchmarks
		let mut batch_results = Vec::with_capacity(batches.len());
		for batch in batches {
			// Get the name of the batch operation
			let name = batch.name.clone();
			let groups = batch.batch_size;
			let iterations = batch.iterations.map(|s| s as u32).unwrap_or(self.samples);
			let skip_batch = ((self.skip_deletes || self.skip_writes)
				&& matches!(batch.operation, crate::BatchOperationType::Delete))
				|| (self.skip_writes
					&& matches!(
						batch.operation,
						crate::BatchOperationType::Create | crate::BatchOperationType::Update
					));
			if skip_batch {
				batch_results.push((name, iterations, groups, None));
				continue;
			}
			// Determine the batch operation type
			let operation = match batch.operation {
				crate::BatchOperationType::Create => BenchmarkOperation::BatchCreate(batch.clone()),
				crate::BatchOperationType::Read => BenchmarkOperation::BatchRead(batch.clone()),
				crate::BatchOperationType::Update => BenchmarkOperation::BatchUpdate(batch.clone()),
				crate::BatchOperationType::Delete => BenchmarkOperation::BatchDelete(batch.clone()),
			};
			// Execute the batch benchmark
			let duration =
				self.run_operation::<C, D>(&clients, operation, kp, vp.clone(), iterations).await?;
			// Store the batch benchmark result
			batch_results.push((name, iterations, groups, duration));
		}
		// Mark the benchmark as complete
		if self.emit_phase_markers {
			self.bench_ui.println_plain("Benchmark complete");
		}
		// Shut down the datastore
		self.wait_for_client(&engine).await?.shutdown().await?;
		// Return the benchmark results
		Ok(BenchmarkResult {
			database,
			system,
			metadata,
			creates,
			reads,
			updates,
			scans: scan_results,
			steady_state: Vec::new(),
			batches: batch_results,
			deletes,
			sample,
		})
	}

	async fn run_steady_state<C>(
		&self,
		clients: &[Arc<C>],
		kp: KeyProvider,
		vp: ValueProvider,
		database: Option<String>,
	) -> Result<Vec<SteadyStateResult>>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let config = SteadyStateConfig::from_benchmark(self)?;
		let workloads = config.workloads()?;
		let mut results = Vec::with_capacity(workloads.len());
		for workload in workloads {
			self.bench_ui.section_header(&format!("Steady-state · {}", workload.name()));
			if let Err(e) = self
				.reset_steady_state_row::<C>(clients, kp, &config, workload, config.records as u64)
				.await
			{
				if e.to_string().eq(NOT_SUPPORTED_ERROR) {
					eprintln!(
						"steady-state pre-run cleanup for {} is unsupported; continuing",
						workload.name()
					);
				} else {
					results.push(failed_steady_state_row(
						self,
						database.clone(),
						&config,
						workload,
						e,
					));
					continue;
				}
			}
			let row = match self
				.run_steady_state_row::<C>(
					clients,
					kp,
					vp.clone(),
					database.clone(),
					&config,
					workload,
				)
				.await
			{
				Ok(row) => row,
				Err(e) if e.to_string().eq(NOT_SUPPORTED_ERROR) => {
					if let Err(reset_error) = self
						.reset_steady_state_row::<C>(
							clients,
							kp,
							&config,
							workload,
							config.records as u64,
						)
						.await
					{
						eprintln!(
							"steady-state error cleanup for {} failed: {reset_error:#}",
							workload.name()
						);
					}
					unsupported_steady_state_row(self, database.clone(), &config, workload, e)
				}
				Err(e) => {
					if let Err(reset_error) = self
						.reset_steady_state_row::<C>(
							clients,
							kp,
							&config,
							workload,
							config.records as u64,
						)
						.await
					{
						eprintln!(
							"steady-state error cleanup for {} failed: {reset_error:#}",
							workload.name()
						);
					}
					failed_steady_state_row(self, database.clone(), &config, workload, e)
				}
			};
			results.push(row);
		}
		Ok(results)
	}

	async fn run_steady_state_row<C>(
		&self,
		clients: &[Arc<C>],
		kp: KeyProvider,
		vp: ValueProvider,
		database: Option<String>,
		config: &SteadyStateConfig,
		workload: SteadyStateWorkload,
	) -> Result<SteadyStateResult>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let prepare_start = Instant::now();
		if workload.requires_prepared_dataset() {
			self.load_steady_state_dataset(clients, kp, vp.clone(), config.records).await?;
		}
		let prepare = completed_phase(prepare_start.elapsed());
		if workload == SteadyStateWorkload::Idle {
			return self
				.run_steady_state_idle_row(clients, kp, config, workload, database, prepare)
				.await;
		}
		let mut next_op_index = 0;

		let warmup_start = Instant::now();
		if config.warmup > Duration::ZERO {
			let warmup = self
				.run_steady_state_window::<C>(
					clients,
					kp,
					vp.clone(),
					config,
					workload,
					config.warmup,
					false,
					next_op_index,
				)
				.await?;
			next_op_index += warmup.completed;
			if let Some(reason) = warmup.failure_reason {
				self.reset_steady_state_row(clients, kp, config, workload, warmup.cleanup_upper)
					.await?;
				bail!(reason);
			}
		}
		let warmup = completed_phase(warmup_start.elapsed());

		let measure_start = Instant::now();
		let measurement = self
			.run_steady_state_window::<C>(
				clients,
				kp,
				vp,
				config,
				workload,
				config.measurement,
				true,
				next_op_index,
			)
			.await?;
		let measure = completed_phase(measure_start.elapsed());
		let measurement_failure = measurement.failure_reason.clone();

		let drain_start = Instant::now();
		self.quiesce_and_mark().await;
		let drain_elapsed = drain_start.elapsed();
		let cleanup_start = Instant::now();
		let cleanup_result = self
			.reset_steady_state_row(clients, kp, config, workload, measurement.cleanup_upper)
			.await;
		let cleanup_elapsed = cleanup_start.elapsed();

		let observed_mix = measurement.observed_mix();
		let result = measurement.result;
		let throughput = result.as_ref().map(|r| SteadyStateThroughput {
			completed_operations: measurement.completed,
			ops_per_sec: r.ops(),
			per_second_windows: measurement
				.per_second_windows
				.iter()
				.map(|count| *count as f64)
				.collect(),
		});
		let latency = result.as_ref().map(|r| SteadyStateLatency {
			sample_count: measurement.latency_samples,
			p50_ms: r.q50() as f64 / 1000.0,
			p95_ms: r.q95() as f64 / 1000.0,
			p99_ms: r.q99() as f64 / 1000.0,
		});
		let validation = Some(SteadyStateValidation {
			errors: measurement.errors,
			read_hits: measurement.read_hits,
			read_misses: measurement.read_misses,
			updates: measurement.updates,
			scan_count_errors: measurement.scan_count_errors,
			observed_mix,
			expected_mix_prefix: workload.expected_mix_prefix(config, measurement.completed),
		});
		let measurement_unsupported = measurement_failure.as_deref() == Some(NOT_SUPPORTED_ERROR);
		let mut status = if measurement_unsupported {
			SteadyStateStatus::Unsupported
		} else if measurement_failure.is_some() {
			SteadyStateStatus::Failed
		} else {
			SteadyStateStatus::Completed
		};
		let mut unsupported_reason =
			measurement_unsupported.then(|| NOT_SUPPORTED_ERROR.to_string());
		let mut failure_reason =
			(!measurement_unsupported).then_some(measurement_failure).flatten();
		let cleanup_status = match cleanup_result {
			Ok(()) => SteadyStateStatus::Completed,
			Err(e) => {
				status = SteadyStateStatus::Failed;
				let cleanup_failure = format!("cleanup failed: {e:#}");
				failure_reason = Some(match failure_reason.or(unsupported_reason.take()) {
					Some(reason) => format!("{reason}; {cleanup_failure}"),
					None => cleanup_failure,
				});
				SteadyStateStatus::Failed
			}
		};
		let cleanup = SteadyStatePhase {
			elapsed_ms: cleanup_elapsed.as_secs_f64() * 1000.0,
			status: cleanup_status,
		};

		Ok(SteadyStateResult {
			name: workload.name().to_string(),
			suite: "steady-state",
			database,
			status,
			unsupported_reason,
			failure_reason,
			sync: self.sync,
			task: workload.task(self, config),
			phases: SteadyStatePhases {
				prepare,
				warmup,
				measure,
				drain: completed_phase(drain_elapsed),
				cleanup,
			},
			throughput,
			latency,
			validation,
			drain: Some(SteadyStateDrain {
				elapsed_ms: drain_elapsed.as_secs_f64() * 1000.0,
				timed_out: false,
			}),
			operation_result: result,
		})
	}

	async fn run_steady_state_idle_row<C>(
		&self,
		clients: &[Arc<C>],
		kp: KeyProvider,
		config: &SteadyStateConfig,
		workload: SteadyStateWorkload,
		database: Option<String>,
		prepare: SteadyStatePhase,
	) -> Result<SteadyStateResult>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let warmup_start = Instant::now();
		tokio::time::sleep(config.warmup).await;
		let warmup = completed_phase(warmup_start.elapsed());
		let measure_start = Instant::now();
		tokio::time::sleep(config.measurement).await;
		let measure = completed_phase(measure_start.elapsed());
		let drain_start = Instant::now();
		self.quiesce_and_mark().await;
		let drain_elapsed = drain_start.elapsed();
		let cleanup_start = Instant::now();
		let cleanup_result =
			self.reset_steady_state_row(clients, kp, config, workload, config.records as u64).await;
		let cleanup_elapsed = cleanup_start.elapsed();
		let (status, unsupported_reason, failure_reason) = match cleanup_result {
			Ok(()) => (SteadyStateStatus::Completed, None, None),
			Err(error) if error.to_string() == NOT_SUPPORTED_ERROR => {
				(SteadyStateStatus::Unsupported, Some(NOT_SUPPORTED_ERROR.to_string()), None)
			}
			Err(error) => {
				(SteadyStateStatus::Failed, None, Some(format!("cleanup failed: {error:#}")))
			}
		};
		Ok(SteadyStateResult {
			name: workload.name().to_string(),
			suite: "steady-state",
			database,
			status,
			unsupported_reason,
			failure_reason,
			sync: self.sync,
			task: workload.task(self, config),
			phases: SteadyStatePhases {
				prepare,
				warmup,
				measure,
				drain: completed_phase(drain_elapsed),
				cleanup: SteadyStatePhase {
					elapsed_ms: cleanup_elapsed.as_secs_f64() * 1000.0,
					status,
				},
			},
			throughput: None,
			latency: None,
			validation: Some(SteadyStateValidation {
				errors: 0,
				read_hits: 0,
				read_misses: 0,
				updates: 0,
				scan_count_errors: 0,
				observed_mix: "none".to_string(),
				expected_mix_prefix: "none".to_string(),
			}),
			drain: Some(SteadyStateDrain {
				elapsed_ms: drain_elapsed.as_secs_f64() * 1000.0,
				timed_out: false,
			}),
			operation_result: None,
		})
	}

	async fn load_steady_state_dataset<C>(
		&self,
		clients: &[Arc<C>],
		kp: KeyProvider,
		vp: ValueProvider,
		records: u32,
	) -> Result<()>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let current = Arc::new(AtomicU32::new(0));
		let mut tasks = JoinSet::new();
		for client in clients {
			for _ in 0..self.threads {
				let client = client.clone();
				let current = current.clone();
				let mut kp = kp;
				let mut vp = vp.clone();
				tasks.spawn(async move {
					loop {
						let key = current.fetch_add(1, Ordering::Relaxed);
						if key >= records {
							break;
						}
						let value = vp.generate_value();
						client.create(key, value, &mut kp).await?;
					}
					Ok::<_, anyhow::Error>(())
				});
			}
		}
		while let Some(result) = tasks.join_next().await {
			result??;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn run_steady_state_window<C>(
		&self,
		clients: &[Arc<C>],
		kp: KeyProvider,
		vp: ValueProvider,
		config: &SteadyStateConfig,
		workload: SteadyStateWorkload,
		duration: Duration,
		record: bool,
		start_op_index: u64,
	) -> Result<SteadyStateMeasurement>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let window_start = Instant::now();
		let deadline = window_start + duration;
		let sequence = Arc::new(AtomicU64::new(0));
		let completed = Arc::new(AtomicU64::new(0));
		let read_hits = Arc::new(AtomicU64::new(0));
		let read_misses = Arc::new(AtomicU64::new(0));
		let updates = Arc::new(AtomicU64::new(0));
		let creates = Arc::new(AtomicU64::new(0));
		let cleanup_upper = Arc::new(AtomicU64::new(start_op_index));
		let scans = Arc::new(AtomicU64::new(0));
		let latency_samples = Arc::new(AtomicU64::new(0));
		let errors = Arc::new(AtomicU64::new(0));
		let scan_count_errors = Arc::new(AtomicU64::new(0));
		let failure_reason = Arc::new(Mutex::new(None::<String>));
		let windows = Arc::new(Mutex::new(Vec::<u64>::new()));
		let metric = record.then(|| OperationMetric::new(self.pid, 0));
		let mut tasks = JoinSet::new();
		for (client_index, client) in clients.iter().cloned().enumerate() {
			for thread_index in 0..self.threads {
				let worker_index = client_index as u64 * self.threads as u64 + thread_index as u64;
				let client = client.clone();
				let sequence = sequence.clone();
				let completed = completed.clone();
				let read_hits = read_hits.clone();
				let read_misses = read_misses.clone();
				let updates = updates.clone();
				let creates = creates.clone();
				let cleanup_upper = cleanup_upper.clone();
				let scans = scans.clone();
				let latency_samples = latency_samples.clone();
				let errors = errors.clone();
				let scan_count_errors = scan_count_errors.clone();
				let failure_reason = failure_reason.clone();
				let windows = windows.clone();
				let mut kp = kp;
				let mut vp = vp.clone();
				let config = config.clone();
				let mut selector = KeySelector::new(&config, workload, worker_index);
				let operation_timeout = self.operation_timeout;
				tasks.spawn(async move {
					let mut histogram = Histogram::new(3)?;
					while Instant::now() < deadline {
						let op_index = sequence.fetch_add(1, Ordering::Relaxed);
						let op = workload.operation_at(&config, op_index);
						let time = Instant::now();
						let op_result = tokio::time::timeout(operation_timeout, async {
							match op {
								SteadyStateOperation::Create => {
									let key_index = start_op_index
										.checked_add(op_index)
										.context("sustained_ingest exceeded u64 key space")?;
									let key = u32::try_from(key_index)
										.context("sustained_ingest exceeded u32 key space")?;
									cleanup_upper.fetch_max(key_index + 1, Ordering::Relaxed);
									let value = vp.generate_value();
									client.create(key, value, &mut kp).await?;
									creates.fetch_add(1, Ordering::Relaxed);
									Ok(())
								}
								SteadyStateOperation::Read => {
									if workload.expects_missing_reads() {
										let key = selector.next_missing_key()?;
										match client.read(key, &mut kp).await {
											Ok(_) => {
												read_hits.fetch_add(1, Ordering::Relaxed);
												Err(anyhow::anyhow!(
													"steady-state {} expected missing key {key}",
													workload.name()
												))
											}
											Err(_) => {
												read_misses.fetch_add(1, Ordering::Relaxed);
												Ok(())
											}
										}
									} else {
										let key = selector.next_key();
										match client.read(key, &mut kp).await {
											Ok(_) => {
												read_hits.fetch_add(1, Ordering::Relaxed);
												Ok(())
											}
											Err(e) => {
												read_misses.fetch_add(1, Ordering::Relaxed);
												Err(e)
											}
										}
									}
								}
								SteadyStateOperation::Update => {
									let key = selector.next_key();
									let value = vp.generate_value();
									client.update(key, value, &mut kp).await?;
									updates.fetch_add(1, Ordering::Relaxed);
									Ok(())
								}
								SteadyStateOperation::Scan => {
									let scan = selector.next_scan();
									client.scan(&scan, &kp, ScanContext::WithoutIndex).await?;
									scans.fetch_add(1, Ordering::Relaxed);
									Ok(())
								}
							}
						})
						.await;
						let op_result = match op_result {
							Ok(result) => result,
							Err(_) => {
								Err(anyhow::anyhow!("steady-state {} timed out", workload.name()))
							}
						};
						if let Err(e) = op_result {
							errors.fetch_add(1, Ordering::Relaxed);
							if matches!(op, SteadyStateOperation::Scan) {
								scan_count_errors.fetch_add(1, Ordering::Relaxed);
							}
							if let Ok(mut failure_reason) = failure_reason.lock()
								&& failure_reason.is_none()
							{
								*failure_reason = Some(e.to_string());
							}
							break;
						}
						let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
						if record && op_index.is_multiple_of(config.latency_sample_every) {
							if let Err(e) = histogram.record(time.elapsed().as_micros() as u64) {
								errors.fetch_add(1, Ordering::Relaxed);
								if let Ok(mut failure_reason) = failure_reason.lock()
									&& failure_reason.is_none()
								{
									*failure_reason = Some(e.to_string());
								}
								break;
							}
							latency_samples.fetch_add(1, Ordering::Relaxed);
						}
						if record {
							let bucket = time.duration_since(window_start).as_secs() as usize;
							if let Ok(mut windows) = windows.lock() {
								if windows.len() <= bucket {
									windows.resize(bucket + 1, 0);
								}
								windows[bucket] += 1;
							}
						}
						let _ = done;
					}
					Ok::<_, anyhow::Error>(histogram)
				});
			}
		}
		let mut global_histogram = Histogram::new(3)?;
		while let Some(result) = tasks.join_next().await {
			global_histogram.add(result??)?;
		}
		let completed = completed.load(Ordering::Relaxed);
		let result = metric.map(|mut metric| {
			metric.set_samples(completed);
			OperationResult::new(metric, global_histogram)
		});
		Ok(SteadyStateMeasurement {
			completed,
			read_hits: read_hits.load(Ordering::Relaxed),
			read_misses: read_misses.load(Ordering::Relaxed),
			updates: updates.load(Ordering::Relaxed),
			creates: creates.load(Ordering::Relaxed),
			scans: scans.load(Ordering::Relaxed),
			errors: errors.load(Ordering::Relaxed),
			scan_count_errors: scan_count_errors.load(Ordering::Relaxed),
			latency_samples: latency_samples.load(Ordering::Relaxed),
			cleanup_upper: cleanup_upper.load(Ordering::Relaxed),
			failure_reason: failure_reason.lock().ok().and_then(|reason| reason.clone()),
			per_second_windows: windows.lock().map(|w| w.clone()).unwrap_or_default(),
			result,
		})
	}

	async fn reset_steady_state_row<C>(
		&self,
		clients: &[Arc<C>],
		kp: KeyProvider,
		config: &SteadyStateConfig,
		workload: SteadyStateWorkload,
		completed_operations: u64,
	) -> Result<()>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let upper = match workload {
			SteadyStateWorkload::SustainedIngest => u32::try_from(completed_operations)
				.context("sustained_ingest cleanup exceeded u32 key space")?,
			_ => config.records,
		};
		if upper == 0 {
			return Ok(());
		}
		let mut kp = kp;
		clients[0].reset_steady_state(upper, &mut kp).await
	}

	/// Build the held-out [`VectorQuerySet`] for a vector-search scan.
	/// Reads N rows (id picked deterministically from `seed`) and extracts the
	/// `field` column. The read cost is paid once here, off the timed window;
	/// the resulting `Vec<f32>` queries are reused across all scan iterations.
	///
	/// Returns `Ok(None)` when the engine cannot surface vector reads (the
	/// holdout extraction hits [`NOT_SUPPORTED_ERROR`]) so the caller can skip
	/// the entire vector scan instead of aborting the benchmark.
	///
	/// Reuses one of the already-connected clients from the benchmark pool
	/// rather than spawning a fresh one — `wait_for_client` carries a
	/// per-engine pre-connect sleep (5s on SurrealDB) that compounds across
	/// the three vector legs.
	async fn build_vector_query_set<C>(
		&self,
		client: &Arc<C>,
		scan: &Scan,
		vq: &VectorQuerySpec,
		mut kp: KeyProvider,
		samples: u32,
	) -> Result<Option<VectorQuerySet>>
	where
		C: BenchmarkClient + Send + Sync,
	{
		let VectorHoldout {
			count,
			seed,
		} = vq.holdout.clone();
		let ids = holdout_indices(samples, count, seed);
		let mut queries = Vec::with_capacity(ids.len());
		for n in ids {
			// Read failures and shape mismatches both mean "this engine can't
			// give us a usable vector for the holdout". Treat both as skip
			// signals so an engine without vector support never aborts the
			// whole benchmark — the scan still records a clean `-` cell,
			// matching how fulltext skips on engines without fulltext. Log
			// the underlying cause so CI runs can tell a real engine bug
			// (worth fixing) apart from an unsupported engine (correct skip).
			let row = match client.read(n, &mut kp).await {
				Ok(r) => r,
				Err(e) => {
					eprintln!("vector holdout: skipping scan `{}` (read: {e:#})", scan.name);
					return Ok(None);
				}
			};
			let bv: BenchValue = row.into();
			match extract_vector_field(&bv, &vq.field) {
				Ok(v) => queries.push(v),
				Err(e) => {
					eprintln!("vector holdout: skipping scan `{}` (extract: {e})", scan.name);
					return Ok(None);
				}
			}
		}
		// Belt-and-suspenders for `VectorQuerySet::pick`'s
		// `sample % queries.len()` — the validator already rejects
		// `holdout.count == 0`, but anything else that ends up returning
		// zero queries (e.g. `samples = 0`) skips the scan cleanly here
		// rather than panicking inside the timed window.
		if queries.is_empty() {
			eprintln!("vector holdout: skipping scan `{}` (empty query set)", scan.name);
			return Ok(None);
		}
		Ok(Some(VectorQuerySet {
			queries: Arc::new(queries),
		}))
	}

	/// Polls until [`BenchmarkEngine::create_client`] succeeds or [`TIMEOUT`] elapses.
	async fn wait_for_client<C, E>(&self, engine: &E) -> Result<C>
	where
		C: BenchmarkClient + Send + Sync,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		// Get the current system time
		let time = SystemTime::now();
		// Get the timeout for the engine
		let wait = engine.wait_timeout();
		// Check the elapsed time
		while time.elapsed()? < TIMEOUT {
			// Wait for a small amount of time
			if let Some(wait) = wait {
				tokio::time::sleep(wait).await
			};
			// Attempt to create a client connection
			match engine.create_client().await {
				Err(e) => debug!("Received error: {e}"),
				Ok(c) => return Ok(c),
			}
		}
		bail!("Can't create the client")
	}

	/// Creates one async connection per logical client; returns shared handles for workers.
	async fn setup_clients<C, E>(&self, engine: &E) -> Result<Vec<Arc<C>>>
	where
		C: BenchmarkClient + Send + Sync,
		E: BenchmarkEngine<C> + Send + Sync,
	{
		// Create a set of client connections
		let mut clients = Vec::with_capacity(self.clients as usize);
		// Create the desired number of connections
		for i in 0..self.clients {
			// Log some information
			info!("Creating client {}", i + 1);
			// Create a new client connection
			clients.push(engine.create_client());
		}
		// Wait for all the clients to connect
		Ok(try_join_all(clients).await?.into_iter().map(Arc::new).collect())
	}

	/// Runs one logical phase across `clients × threads` workers with shared progress and metrics.
	async fn run_operation<C, D>(
		&self,
		clients: &[Arc<C>],
		operation: BenchmarkOperation,
		kp: KeyProvider,
		vp: ValueProvider,
		samples: u32,
	) -> Result<Option<OperationResult>>
	where
		C: BenchmarkClient + Send + Sync,
		D: Dialect,
	{
		// Optional line for log-based profiling (`dev.sh`, grep over captured logs).
		// `phase_marker_label` includes the scan id / run name / ctx so per-scan and
		// per-index DDL windows are uniquely greppable.
		if self.emit_phase_markers {
			self.bench_ui.println_plain(&format!("{} starting", phase_marker_label(&operation)));
		}
		let progress =
			self.bench_ui.progress_bar(samples as u64, &progress_short_label(&operation));
		// Whether we have experienced an error
		let error = Arc::new(AtomicBool::new(false));
		// Wether the test should be skipped
		let skip = Arc::new(AtomicBool::new(false));
		// The total records processed so far
		let current = Arc::new(AtomicU32::new(0));
		// The total records processed so far
		let complete = Arc::new(AtomicU32::new(0));
		// Store the worker tasks in a join set so failures can stop the operation promptly.
		let mut tasks = JoinSet::new();
		// Measure the starting time
		let metric = OperationMetric::new(self.pid, samples as u64);
		// Loop over the clients
		for (client, _) in clients.iter().cloned().zip(1..) {
			// Loop over the threads
			for _ in 0..self.threads {
				let error = error.clone();
				let skip = skip.clone();
				let current = current.clone();
				let complete = complete.clone();
				let client = client.clone();
				let progress = progress.clone();
				let vp = vp.clone();
				let operation = operation.clone();
				let operation_timeout = self.operation_timeout;
				tasks.spawn(async move {
					match Self::operation_loop::<C, D>(
						client,
						samples,
						&error,
						&current,
						&complete,
						operation,
						operation_timeout,
						(kp, vp, progress),
					)
					.await
					{
						Err(e) if e.to_string().eq(NOT_SUPPORTED_ERROR) => {
							skip.store(true, Ordering::Relaxed);
							Ok(None)
						}
						Err(e) => {
							eprintln!("{e}");
							error.store(true, Ordering::Relaxed);
							Err(e)
						}
						Ok(h) => Ok(Some(h)),
					}
				});
			}
		}
		// Wait for the threads to complete, aborting the remaining tasks on the first failure.
		let mut global_histogram = Histogram::new(3)?;
		while let Some(result) = tasks.join_next().await {
			match result {
				Ok(Ok(Some(histogram))) => {
					global_histogram.add(histogram)?;
				}
				Ok(Ok(None)) => {}
				Ok(Err(e)) => {
					error.store(true, Ordering::Relaxed);
					tasks.abort_all();
					while tasks.join_next().await.is_some() {}
					if let Some(ref pb) = progress {
						pb.finish_and_clear();
					}
					return Err(e).with_context(|| format!("{operation} worker failed"));
				}
				Err(e) => {
					error.store(true, Ordering::Relaxed);
					tasks.abort_all();
					while tasks.join_next().await.is_some() {}
					if let Some(ref pb) = progress {
						pb.finish_and_clear();
					}
					return Err(e).with_context(|| format!("{operation} task failed"));
				}
			}
		}
		// Finish the progress bar at 100% before tearing it down
		if let Some(ref pb) = progress {
			pb.set_position(samples as u64);
			pb.finish_and_clear();
		}
		if error.load(Ordering::Relaxed) {
			bail!("Task failure");
		}
		// Histogram + sysinfo snapshots → OperationResult; then print phase timing line
		let result = OperationResult::new(metric, global_histogram);
		let took = result.total_time();
		match &operation {
			BenchmarkOperation::Scan(_, ctx) => {
				self.bench_ui.println_took_scan(scan_context_slug(*ctx), None, &took);
			}
			BenchmarkOperation::VectorScan(_, ctx, _) => {
				self.bench_ui.println_took_scan(scan_context_slug(*ctx), None, &took);
			}
			BenchmarkOperation::ScanWithWrites(_, ctx, spec) => {
				self.bench_ui.println_took_scan(
					scan_context_slug(*ctx),
					Some(writes_ratio_percent(spec)),
					&took,
				);
			}
			_ => {
				// Create/Read/Update/Delete, index DDL, and batch ops share the default line format
				self.bench_ui.println_took_head(&operation.to_string(), &took);
			}
		}
		// Grep-friendly took marker for ops whose UI line collapses multiple
		// runs onto the same label (scans always reuse `Scan :: no-index`/
		// `Scan :: indexed`; BuildIndex/RemoveIndex reuse their bare name).
		// The rich marker disambiguates by scan id so dev.sh can attach one
		// perf window per run.
		if self.emit_phase_markers
			&& matches!(
				&operation,
				BenchmarkOperation::Scan(..)
					| BenchmarkOperation::ScanWithWrites(..)
					| BenchmarkOperation::BuildIndex(..)
					| BenchmarkOperation::RemoveIndex(..)
			) {
			self.bench_ui.println_plain(&format!(
				"{} took {}",
				phase_marker_label(&operation),
				took
			));
		}
		// Shall we skip the operation? (operation not supported)
		if skip.load(Ordering::Relaxed) {
			return Ok(None);
		}
		// Wait for server-side phase tail to drain and emit the
		// `Server idle` marker. Must happen *after* the took line so
		// dev.sh sees took → Server idle → (next phase) starting.
		self.quiesce_and_mark().await;
		// Everything ok
		Ok(Some(result))
	}

	#[allow(clippy::too_many_arguments)]
	/// Per-worker loop: claim sample indices until done; record microsecond latencies in a histogram.
	async fn operation_loop<C, D>(
		client: Arc<C>,
		samples: u32,
		error: &AtomicBool,
		current: &AtomicU32,
		complete: &AtomicU32,
		operation: BenchmarkOperation,
		operation_timeout: Duration,
		(mut kp, mut vp, progress): (KeyProvider, ValueProvider, Option<Arc<ProgressBar>>),
	) -> Result<Histogram<u64>>
	where
		C: BenchmarkClient,
		D: Dialect,
	{
		let mut histogram = Histogram::new(3)?;
		// Check if we have encountered an error
		while !error.load(Ordering::Relaxed) {
			// Get the current sample number
			let sample = current.fetch_add(1, Ordering::Relaxed);
			// Have we produced enough samples
			if sample >= samples {
				// We are done
				break;
			}
			// Perform the benchmark operation under a per-iteration
			// timeout. A stuck `await` inside the underlying SDK
			// (e.g. a WebSocket reply that never lands because the
			// connection was torn down without completing the
			// matching oneshot) returns an error here instead of
			// parking the worker task forever; the operation `JoinSet` then
			// short-circuits with the operation name in the error
			// chain rather than hanging in `block_on`.
			let time = Instant::now();
			tokio::time::timeout(operation_timeout, async {
				match &operation {
					BenchmarkOperation::Create => {
						let value = vp.generate_value();
						client.create(sample, value, &mut kp).await
					}
					BenchmarkOperation::Read => client.read(sample, &mut kp).await.map(|_| ()),
					BenchmarkOperation::Update => {
						let value = vp.generate_value();
						client.update(sample, value, &mut kp).await
					}
					BenchmarkOperation::Scan(s, ctx) => client.scan(s, &kp, *ctx).await,
					BenchmarkOperation::VectorScan(s, ctx, qs) => {
						let q = qs.pick(sample);
						client.scan_vector(s, q, &kp, *ctx).await
					}
					BenchmarkOperation::ScanWithWrites(scan, ctx, spec) => {
						workloads::run_scan_with_writes(
							&*client, scan, *ctx, spec, sample, samples, &mut kp,
						)
						.await
					}
					BenchmarkOperation::BuildIndex(spec, id, _) => {
						client.build_index(spec, id.as_str()).await
					}
					BenchmarkOperation::BuildVectorIndex(spec, vq, dim, name) => {
						client.build_vector_index(spec, vq, *dim, name.as_str()).await
					}
					BenchmarkOperation::RemoveIndex(id, _) => client.drop_index(id.as_str()).await,
					BenchmarkOperation::Delete => client.delete(sample, &mut kp).await,
					BenchmarkOperation::BatchCreate(batch_op) => {
						client.batch_create(sample, batch_op, &mut kp, &mut vp).await
					}
					BenchmarkOperation::BatchRead(batch_op) => {
						client.batch_read(sample, batch_op, &mut kp).await
					}
					BenchmarkOperation::BatchUpdate(batch_op) => {
						client.batch_update(sample, batch_op, &mut kp, &mut vp).await
					}
					BenchmarkOperation::BatchDelete(batch_op) => {
						client.batch_delete(sample, batch_op, &mut kp).await
					}
				}
			})
			.await
			.with_context(|| {
				format!("{operation} did not complete within {operation_timeout:?}")
			})??;
			// Get the completed sample number
			let sample = complete.fetch_add(1, Ordering::Relaxed);
			if let Some(pb) = &progress {
				let done = ((sample + 1).min(samples)) as u64;
				pb.set_position(done);
			}
			histogram.record(time.elapsed().as_micros() as u64)?;
		}
		Ok(histogram)
	}
}

#[derive(Clone)]
struct SteadyStateConfig {
	records: u32,
	warmup: Duration,
	measurement: Duration,
	latency_sample_every: u64,
	seed: u64,
	zipfian_exponent: f64,
	bench_spec: Option<String>,
	operation_mix: Option<String>,
	operation_schedule: Option<Vec<SteadyStateOperation>>,
	operation_mix_period: u32,
}

impl SteadyStateConfig {
	fn from_benchmark(benchmark: &Benchmark) -> Result<Self> {
		if benchmark.operation_mix_period == 0 {
			bail!("--operation-mix-period must be greater than 0");
		}
		let (records, warmup, measurement, latency_sample_every) =
			match benchmark.steady_state_preset {
				SteadyStatePreset::Smoke => (benchmark.samples, 0, 1, 1),
				SteadyStatePreset::Default => (benchmark.samples, 30, 120, 100),
				SteadyStatePreset::Large => (benchmark.samples, 60, 300, 100),
			};
		let operation_schedule = benchmark
			.operation_mix
			.as_deref()
			.map(|spec| parse_operation_mix(spec, benchmark.operation_mix_period))
			.transpose()?;
		Ok(Self {
			records,
			warmup: Duration::from_secs(benchmark.warmup_secs.unwrap_or(warmup)),
			measurement: Duration::from_secs(benchmark.measurement_secs.unwrap_or(measurement)),
			latency_sample_every: benchmark
				.latency_sample_every
				.unwrap_or(latency_sample_every)
				.max(1),
			seed: benchmark.seed,
			zipfian_exponent: benchmark.zipfian_exponent,
			bench_spec: benchmark.steady_state_benches.clone(),
			operation_mix: benchmark.operation_mix.clone(),
			operation_schedule,
			operation_mix_period: benchmark.operation_mix_period,
		})
	}

	fn workloads(&self) -> Result<Vec<SteadyStateWorkload>> {
		let workloads: Vec<SteadyStateWorkload> = match self.bench_spec.as_deref() {
			None => DEFAULT_STEADY_STATE_WORKLOADS.to_vec(),
			Some(spec) => spec
				.split(',')
				.map(str::trim)
				.filter(|name| !name.is_empty())
				.map(SteadyStateWorkload::from_name)
				.collect::<Result<_>>()?,
		};
		if self.operation_schedule.is_none()
			&& workloads.contains(&SteadyStateWorkload::BalancedZipfian)
			&& !self.operation_mix_period.is_multiple_of(2)
		{
			bail!(
				"balanced_zipfian requires an even --operation-mix-period for exact read=0.5,update=0.5"
			);
		}
		if self.operation_schedule.is_none()
			&& workloads.iter().any(|workload| {
				matches!(
					workload,
					SteadyStateWorkload::ReadHeavyZipfian | SteadyStateWorkload::UpdateHeavyZipfian
				)
			}) && !self.operation_mix_period.is_multiple_of(20)
		{
			bail!(
				"read_heavy_zipfian and update_heavy_zipfian require --operation-mix-period to be a multiple of 20 for exact 95/5 mixes"
			);
		}
		if self
			.operation_schedule
			.as_ref()
			.is_some_and(|schedule| schedule.contains(&SteadyStateOperation::Create))
			&& workloads.iter().any(|workload| *workload != SteadyStateWorkload::SustainedIngest)
		{
			bail!(
				"custom steady-state operation mixes may include create only for sustained_ingest"
			);
		}
		if let Some(schedule) = &self.operation_schedule {
			for workload in &workloads {
				workload.validate_operation_schedule(schedule)?;
			}
		}
		Ok(workloads)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SteadyStateWorkload {
	BalancedZipfian,
	ReadHeavyZipfian,
	UpdateHeavyZipfian,
	PointReadZipfian,
	PointReadUniform,
	PointReadMissingInRange,
	RangeScanUniform,
	SustainedIngest,
	Idle,
}

impl SteadyStateWorkload {
	fn name(self) -> &'static str {
		match self {
			Self::BalancedZipfian => "balanced_zipfian",
			Self::ReadHeavyZipfian => "read_heavy_zipfian",
			Self::UpdateHeavyZipfian => "update_heavy_zipfian",
			Self::PointReadZipfian => "point_read_zipfian",
			Self::PointReadUniform => "point_read_uniform",
			Self::PointReadMissingInRange => "point_read_missing_in_range",
			Self::RangeScanUniform => "range_scan_uniform",
			Self::SustainedIngest => "sustained_ingest",
			Self::Idle => "idle",
		}
	}

	fn from_name(name: &str) -> Result<Self> {
		match name {
			"balanced_zipfian" => Ok(Self::BalancedZipfian),
			"read_heavy_zipfian" => Ok(Self::ReadHeavyZipfian),
			"update_heavy_zipfian" => Ok(Self::UpdateHeavyZipfian),
			"point_read_zipfian" => Ok(Self::PointReadZipfian),
			"point_read_uniform" => Ok(Self::PointReadUniform),
			"point_read_missing_in_range" => Ok(Self::PointReadMissingInRange),
			"range_scan_uniform" => Ok(Self::RangeScanUniform),
			"sustained_ingest" => Ok(Self::SustainedIngest),
			"idle" => Ok(Self::Idle),
			other => bail!("unsupported steady-state workload `{other}`"),
		}
	}

	fn requires_prepared_dataset(self) -> bool {
		!matches!(self, Self::SustainedIngest)
	}

	fn operation_at(self, config: &SteadyStateConfig, op_index: u64) -> SteadyStateOperation {
		if let Some(schedule) = &config.operation_schedule {
			return schedule[(op_index % schedule.len() as u64) as usize];
		}
		match self {
			Self::BalancedZipfian => {
				let period_slot = op_index % u64::from(config.operation_mix_period);
				let half = u64::from(config.operation_mix_period / 2);
				if period_slot < half {
					SteadyStateOperation::Read
				} else {
					SteadyStateOperation::Update
				}
			}
			Self::ReadHeavyZipfian => {
				let period_slot = op_index % u64::from(config.operation_mix_period);
				let read_slots = u64::from(config.operation_mix_period) * 19 / 20;
				if period_slot < read_slots {
					SteadyStateOperation::Read
				} else {
					SteadyStateOperation::Update
				}
			}
			Self::UpdateHeavyZipfian => {
				let period_slot = op_index % u64::from(config.operation_mix_period);
				let read_slots = u64::from(config.operation_mix_period / 20);
				if period_slot < read_slots {
					SteadyStateOperation::Read
				} else {
					SteadyStateOperation::Update
				}
			}
			Self::PointReadZipfian => SteadyStateOperation::Read,
			Self::PointReadUniform => SteadyStateOperation::Read,
			Self::PointReadMissingInRange => SteadyStateOperation::Read,
			Self::RangeScanUniform => SteadyStateOperation::Scan,
			Self::SustainedIngest => SteadyStateOperation::Create,
			Self::Idle => unreachable!("idle workloads do not execute client operations"),
		}
	}

	fn operation_mix(self, config: &SteadyStateConfig) -> String {
		if let Some(spec) = &config.operation_mix {
			return spec.clone();
		}
		match self {
			Self::BalancedZipfian => "read=0.5,update=0.5".to_string(),
			Self::ReadHeavyZipfian => "read=0.95,update=0.05".to_string(),
			Self::UpdateHeavyZipfian => "read=0.05,update=0.95".to_string(),
			Self::PointReadZipfian => "read=1.0".to_string(),
			Self::PointReadUniform => "read=1.0".to_string(),
			Self::PointReadMissingInRange => "read=1.0".to_string(),
			Self::RangeScanUniform => "scan=1.0".to_string(),
			Self::SustainedIngest => "create=1.0".to_string(),
			Self::Idle => "none".to_string(),
		}
	}

	fn key_selection(self) -> &'static str {
		match self {
			Self::PointReadUniform => "uniform",
			Self::PointReadMissingInRange => "scrambled_zipfian_missing_range",
			Self::RangeScanUniform => "uniform_positional",
			Self::SustainedIngest => "unique_sequential",
			Self::Idle => "none",
			_ => "scrambled_zipfian",
		}
	}

	fn validate_operation_schedule(self, schedule: &[SteadyStateOperation]) -> Result<()> {
		let valid = match self {
			Self::SustainedIngest => {
				schedule.iter().all(|op| matches!(op, SteadyStateOperation::Create))
			}
			Self::RangeScanUniform => {
				schedule.iter().all(|op| matches!(op, SteadyStateOperation::Scan))
			}
			Self::BalancedZipfian
			| Self::ReadHeavyZipfian
			| Self::UpdateHeavyZipfian
			| Self::PointReadZipfian
			| Self::PointReadUniform => schedule
				.iter()
				.all(|op| matches!(op, SteadyStateOperation::Read | SteadyStateOperation::Update)),
			Self::PointReadMissingInRange => {
				schedule.iter().all(|op| matches!(op, SteadyStateOperation::Read))
			}
			Self::Idle => false,
		};
		if !valid {
			bail!(
				"custom steady-state operation mix is incompatible with {} key-selection contract",
				self.name()
			);
		}
		Ok(())
	}

	fn task(self, benchmark: &Benchmark, config: &SteadyStateConfig) -> SteadyStateTask {
		SteadyStateTask {
			records: config.records,
			clients: benchmark.clients,
			threads: benchmark.threads,
			warmup_secs: config.warmup.as_secs(),
			measurement_secs: config.measurement.as_secs(),
			latency_sample_every: config.latency_sample_every,
			seed: config.seed,
			worker_seed_derivation: "splitmix64(seed ^ worker_index)",
			operation_mix: self.operation_mix(config),
			operation_mix_period: config.operation_mix_period,
			key_selection: self.key_selection(),
			zipfian_exponent: config.zipfian_exponent,
		}
	}

	fn expected_mix_prefix(self, config: &SteadyStateConfig, completed: u64) -> String {
		if self == SteadyStateWorkload::Idle {
			return "none".to_string();
		}
		let mut counts = SteadyStateCounts::default();
		let period = config
			.operation_schedule
			.as_ref()
			.map_or(config.operation_mix_period as u64, |schedule| schedule.len() as u64);
		let full_periods = completed / period;
		let remainder = (completed % period) as usize;
		let add_remainder = if let Some(schedule) = &config.operation_schedule {
			for operation in schedule {
				counts.add_n(*operation, full_periods);
			}
			true
		} else {
			match self {
				Self::BalancedZipfian => {
					let reads_per_period = config.operation_mix_period as u64 / 2;
					counts.reads = full_periods * reads_per_period;
					counts.updates = full_periods * reads_per_period;
					true
				}
				Self::ReadHeavyZipfian | Self::UpdateHeavyZipfian => {
					let read_slots = match self {
						Self::ReadHeavyZipfian => config.operation_mix_period as u64 * 19 / 20,
						Self::UpdateHeavyZipfian => config.operation_mix_period as u64 / 20,
						_ => unreachable!(),
					};
					let full_reads = full_periods * read_slots;
					let full_updates = full_periods * (period - read_slots);
					counts.reads = full_reads;
					counts.updates = full_updates;
					true
				}
				_ => {
					counts.add_n(self.operation_at(config, 0), completed);
					false
				}
			}
		};
		if add_remainder {
			for operation in 0..remainder as u64 {
				counts.add(self.operation_at(config, full_periods * period + operation));
			}
		}
		counts.to_mix_string()
	}

	fn expects_missing_reads(self) -> bool {
		matches!(self, Self::PointReadMissingInRange)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SteadyStateOperation {
	Create,
	Read,
	Update,
	Scan,
}

#[derive(Default)]
struct SteadyStateCounts {
	creates: u64,
	reads: u64,
	updates: u64,
	scans: u64,
}

impl SteadyStateCounts {
	fn add_n(&mut self, operation: SteadyStateOperation, count: u64) {
		match operation {
			SteadyStateOperation::Create => self.creates += count,
			SteadyStateOperation::Read => self.reads += count,
			SteadyStateOperation::Update => self.updates += count,
			SteadyStateOperation::Scan => self.scans += count,
		}
	}
	fn add(&mut self, op: SteadyStateOperation) {
		match op {
			SteadyStateOperation::Create => self.creates += 1,
			SteadyStateOperation::Read => self.reads += 1,
			SteadyStateOperation::Update => self.updates += 1,
			SteadyStateOperation::Scan => self.scans += 1,
		}
	}

	fn to_mix_string(&self) -> String {
		let mut parts = Vec::new();
		if self.creates > 0 {
			parts.push(format!("create={}", self.creates));
		}
		if self.reads > 0 {
			parts.push(format!("read={}", self.reads));
		}
		if self.updates > 0 {
			parts.push(format!("update={}", self.updates));
		}
		if self.scans > 0 {
			parts.push(format!("scan={}", self.scans));
		}
		parts.join(",")
	}
}

#[derive(Default)]
struct SteadyStateMeasurement {
	completed: u64,
	read_hits: u64,
	read_misses: u64,
	updates: u64,
	creates: u64,
	scans: u64,
	errors: u64,
	scan_count_errors: u64,
	latency_samples: u64,
	cleanup_upper: u64,
	failure_reason: Option<String>,
	per_second_windows: Vec<u64>,
	result: Option<OperationResult>,
}

impl SteadyStateMeasurement {
	fn observed_mix(&self) -> String {
		SteadyStateCounts {
			creates: self.creates,
			reads: self.read_hits + self.read_misses,
			updates: self.updates,
			scans: self.scans,
		}
		.to_mix_string()
	}
}

struct KeySelector {
	records: u32,
	scan_width: usize,
	rng: u64,
	zipf_cdf: Vec<f64>,
	scrambled_keys: Vec<u32>,
}

impl KeySelector {
	fn new(config: &SteadyStateConfig, workload: SteadyStateWorkload, worker_index: u64) -> Self {
		let rng = splitmix64(config.seed ^ worker_index);
		let uses_zipfian = matches!(
			workload,
			SteadyStateWorkload::BalancedZipfian
				| SteadyStateWorkload::ReadHeavyZipfian
				| SteadyStateWorkload::UpdateHeavyZipfian
				| SteadyStateWorkload::PointReadZipfian
				| SteadyStateWorkload::PointReadMissingInRange
		);
		let zipf_cdf = if uses_zipfian {
			build_zipf_cdf(config.records, config.zipfian_exponent)
		} else {
			Vec::new()
		};
		let scrambled_keys = if uses_zipfian {
			build_scrambled_keys(config.records, config.seed)
		} else {
			Vec::new()
		};
		Self {
			records: config.records,
			scan_width: 100,
			rng,
			zipf_cdf,
			scrambled_keys,
		}
	}

	fn next_key(&mut self) -> u32 {
		let rank = if self.zipf_cdf.is_empty() {
			self.uniform_u32(self.records.max(1))
		} else {
			let sample = self.uniform_f64();
			self.zipf_cdf
				.partition_point(|p| *p < sample)
				.min(self.zipf_cdf.len().saturating_sub(1)) as u32
		};
		if self.scrambled_keys.is_empty() {
			rank.min(self.records.saturating_sub(1))
		} else {
			self.scrambled_keys[rank as usize]
		}
	}

	fn next_missing_key(&mut self) -> Result<u32> {
		self.records
			.checked_add(self.next_key())
			.context("missing-read key range exceeded u32 key space")
	}

	fn next_scan(&mut self) -> Scan {
		let max_start = self.records.saturating_sub(self.scan_width as u32);
		let start = self.uniform_u32(max_start.saturating_add(1)) as usize;
		Scan {
			id: "range_scan_uniform".to_string(),
			spec_group: 0,
			multi_run_spec: false,
			name: "range_scan_uniform".to_string(),
			iterations: None,
			condition: None,
			order_by: None,
			start: Some(start),
			limit: Some(self.scan_width),
			expect: Some(self.scan_width.min(self.records as usize)),
			projection: Some("ID".to_string()),
			with_index: None,
			with_writes: Vec::new(),
			vector_query: None,
		}
	}

	fn uniform_u32(&mut self, upper: u32) -> u32 {
		if upper == 0 {
			return 0;
		}
		let upper = upper as u64;
		let threshold = upper.wrapping_neg() % upper;
		loop {
			let value = self.next_u64();
			if value >= threshold {
				return (value % upper) as u32;
			}
		}
	}

	fn uniform_f64(&mut self) -> f64 {
		const DENOMINATOR: f64 = u64::MAX as f64;
		self.next_u64() as f64 / DENOMINATOR
	}

	fn next_u64(&mut self) -> u64 {
		self.rng = splitmix64(self.rng);
		self.rng
	}
}

fn build_zipf_cdf(records: u32, exponent: f64) -> Vec<f64> {
	let records = records.max(1) as usize;
	let mut weights = Vec::with_capacity(records);
	let mut total = 0.0;
	for rank in 1..=records {
		let weight = 1.0 / (rank as f64).powf(exponent);
		total += weight;
		weights.push(total);
	}
	for value in &mut weights {
		*value /= total;
	}
	weights
}

fn build_scrambled_keys(records: u32, seed: u64) -> Vec<u32> {
	let records = records.max(1);
	let mut keys: Vec<_> = (0..records).collect();
	let mut rng = splitmix64(seed ^ 0xD1B5_4A32_D192_ED03);
	for i in (1..keys.len()).rev() {
		rng = splitmix64(rng);
		keys.swap(i, (rng as usize) % (i + 1));
	}
	if keys.len() > 1 && keys[0] == 0 {
		keys.swap(0, 1);
	}
	keys
}

fn splitmix64(mut value: u64) -> u64 {
	value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
	let mut z = value;
	z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
	z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
	z ^ (z >> 31)
}

fn parse_operation_mix(spec: &str, period: u32) -> Result<Vec<SteadyStateOperation>> {
	if period > MAX_OPERATION_MIX_PERIOD {
		bail!("operation mix period cannot exceed {MAX_OPERATION_MIX_PERIOD}");
	}
	let mut total = 0.0;
	let mut schedule = Vec::new();
	for part in spec.split(',') {
		let Some((name, value)) = part.split_once('=') else {
			bail!("operation mix part `{part}` must use name=value");
		};
		let op = match name {
			"read" => SteadyStateOperation::Read,
			"update" => SteadyStateOperation::Update,
			"create" => SteadyStateOperation::Create,
			"scan" => SteadyStateOperation::Scan,
			other => bail!("unsupported operation `{other}` in operation mix"),
		};
		let ratio = value.parse::<f64>()?;
		if !ratio.is_finite() || ratio <= 0.0 {
			bail!("operation mix part `{part}` must be a positive finite ratio");
		}
		total += ratio;
		let count = ratio * period as f64;
		if (count.round() - count).abs() > 0.000_001 {
			bail!("operation mix part `{part}` cannot be represented exactly by period {period}");
		}
		schedule.extend(std::iter::repeat_n(op, count.round() as usize));
	}
	if (total - 1.0).abs() > 0.000_001 {
		bail!("operation mix must sum to 1.0");
	}
	if schedule.len() != period as usize {
		bail!("operation mix expands to {} slots, expected {period}", schedule.len());
	}
	Ok(schedule)
}

fn completed_phase(elapsed: Duration) -> SteadyStatePhase {
	SteadyStatePhase {
		elapsed_ms: elapsed.as_secs_f64() * 1000.0,
		status: SteadyStateStatus::Completed,
	}
}

fn unsupported_steady_state_row(
	benchmark: &Benchmark,
	database: Option<String>,
	config: &SteadyStateConfig,
	workload: SteadyStateWorkload,
	error: anyhow::Error,
) -> SteadyStateResult {
	let phase = SteadyStatePhase {
		elapsed_ms: 0.0,
		status: SteadyStateStatus::Unsupported,
	};
	SteadyStateResult {
		name: workload.name().to_string(),
		suite: "steady-state",
		database,
		status: SteadyStateStatus::Unsupported,
		unsupported_reason: Some(error.to_string()),
		failure_reason: None,
		sync: benchmark.sync,
		task: workload.task(benchmark, config),
		phases: SteadyStatePhases {
			prepare: phase.clone(),
			warmup: phase.clone(),
			measure: phase.clone(),
			drain: phase.clone(),
			cleanup: phase,
		},
		throughput: None,
		latency: None,
		validation: None,
		drain: None,
		operation_result: None,
	}
}

fn failed_steady_state_row(
	benchmark: &Benchmark,
	database: Option<String>,
	config: &SteadyStateConfig,
	workload: SteadyStateWorkload,
	error: anyhow::Error,
) -> SteadyStateResult {
	let phase = SteadyStatePhase {
		elapsed_ms: 0.0,
		status: SteadyStateStatus::Failed,
	};
	SteadyStateResult {
		name: workload.name().to_string(),
		suite: "steady-state",
		database,
		status: SteadyStateStatus::Failed,
		unsupported_reason: None,
		failure_reason: Some(error.to_string()),
		sync: benchmark.sync,
		task: workload.task(benchmark, config),
		phases: SteadyStatePhases {
			prepare: phase.clone(),
			warmup: phase.clone(),
			measure: phase.clone(),
			drain: phase.clone(),
			cleanup: phase,
		},
		throughput: None,
		latency: None,
		validation: Some(SteadyStateValidation {
			errors: 1,
			read_hits: 0,
			read_misses: 0,
			updates: 0,
			scan_count_errors: 0,
			observed_mix: String::new(),
			expected_mix_prefix: workload.expected_mix_prefix(config, 0),
		}),
		drain: None,
		operation_result: None,
	}
}

#[cfg(test)]
mod steady_state_tests {
	use super::*;

	#[test]
	fn operation_mix_requires_exact_period() {
		let err = parse_operation_mix("read=0.333,update=0.667", 100).unwrap_err();
		assert!(err.to_string().contains("cannot be represented exactly"));
	}

	#[test]
	fn operation_mix_builds_canonical_schedule() -> Result<()> {
		let schedule = parse_operation_mix("read=0.5,update=0.5", 10)?;
		assert!(matches!(schedule[0], SteadyStateOperation::Read));
		assert!(matches!(schedule[4], SteadyStateOperation::Read));
		assert!(matches!(schedule[5], SteadyStateOperation::Update));
		assert!(matches!(schedule[9], SteadyStateOperation::Update));
		Ok(())
	}

	#[test]
	fn operation_mix_rejects_invalid_ratios() {
		for spec in ["read=NaN,update=1.0", "read=inf", "read=-1.0,update=2.0", "read=0.0"] {
			let err = parse_operation_mix(spec, 10).expect_err("invalid ratio fails");
			assert!(err.to_string().contains("positive finite ratio"));
		}
	}

	#[test]
	fn operation_mix_rejects_huge_period() {
		let err = parse_operation_mix("read=1.0", MAX_OPERATION_MIX_PERIOD + 1)
			.expect_err("huge period fails");

		assert!(err.to_string().contains("cannot exceed"));
	}

	#[test]
	fn expected_mix_uses_partial_prefix() {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: None,
			operation_mix: None,
			operation_schedule: None,
			operation_mix_period: 10,
		};
		let prefix = SteadyStateWorkload::BalancedZipfian.expected_mix_prefix(&config, 13);
		assert_eq!(prefix, "read=8,update=5");
		assert_eq!(
			SteadyStateWorkload::PointReadZipfian.expected_mix_prefix(&config, 13),
			"read=13"
		);
		assert_eq!(
			SteadyStateWorkload::SustainedIngest.expected_mix_prefix(&config, 13),
			"create=13"
		);
	}

	#[test]
	fn balanced_zipfian_rejects_odd_period() {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: Some("balanced_zipfian".to_string()),
			operation_mix: None,
			operation_schedule: None,
			operation_mix_period: 999,
		};
		let err = config.workloads().unwrap_err();
		assert!(err.to_string().contains("requires an even"));
	}

	#[test]
	fn default_steady_state_rows_match_gateable_rows() -> Result<()> {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: None,
			operation_mix: None,
			operation_schedule: None,
			operation_mix_period: 1000,
		};
		let rows: Vec<_> = config.workloads()?.into_iter().map(SteadyStateWorkload::name).collect();

		assert_eq!(
			rows,
			vec![
				"balanced_zipfian",
				"read_heavy_zipfian",
				"update_heavy_zipfian",
				"point_read_zipfian",
				"point_read_uniform",
				"point_read_missing_in_range",
				"range_scan_uniform",
				"sustained_ingest"
			]
		);
		Ok(())
	}

	#[test]
	fn multi_row_steady_state_is_allowed() -> Result<()> {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: Some("balanced_zipfian,point_read_zipfian".to_string()),
			operation_mix: None,
			operation_schedule: None,
			operation_mix_period: 1000,
		};
		let workloads = config.workloads()?;
		assert_eq!(
			workloads,
			vec![SteadyStateWorkload::BalancedZipfian, SteadyStateWorkload::PointReadZipfian]
		);
		Ok(())
	}

	#[test]
	fn follow_up_zipfian_rows_are_allowed() -> Result<()> {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: Some("read_heavy_zipfian,update_heavy_zipfian".to_string()),
			operation_mix: None,
			operation_schedule: None,
			operation_mix_period: 1000,
		};
		let workloads = config.workloads()?;

		assert_eq!(
			workloads,
			vec![SteadyStateWorkload::ReadHeavyZipfian, SteadyStateWorkload::UpdateHeavyZipfian]
		);
		assert_eq!(
			SteadyStateWorkload::ReadHeavyZipfian.expected_mix_prefix(&config, 1000),
			"read=950,update=50"
		);
		assert_eq!(
			SteadyStateWorkload::UpdateHeavyZipfian.expected_mix_prefix(&config, 1000),
			"read=50,update=950"
		);
		Ok(())
	}

	#[test]
	fn follow_up_zipfian_rows_use_zipfian_selector() {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: None,
			operation_mix: None,
			operation_schedule: None,
			operation_mix_period: 1000,
		};

		let read_heavy = KeySelector::new(&config, SteadyStateWorkload::ReadHeavyZipfian, 0);
		let update_heavy = KeySelector::new(&config, SteadyStateWorkload::UpdateHeavyZipfian, 0);
		let uniform = KeySelector::new(&config, SteadyStateWorkload::PointReadUniform, 0);

		assert!(!read_heavy.zipf_cdf.is_empty());
		assert!(!update_heavy.zipf_cdf.is_empty());
		assert!(!read_heavy.scrambled_keys.is_empty());
		assert!(uniform.zipf_cdf.is_empty());
		assert!(uniform.scrambled_keys.is_empty());
		assert_ne!(read_heavy.scrambled_keys[0], 0);
	}

	#[test]
	fn uniform_selector_stays_within_bounds() {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: None,
			operation_mix: None,
			operation_schedule: None,
			operation_mix_period: 1000,
		};
		let mut selector = KeySelector::new(&config, SteadyStateWorkload::PointReadUniform, 0);

		for _ in 0..10_000 {
			assert!(selector.uniform_u32(37) < 37);
		}
	}

	#[test]
	fn follow_up_zipfian_rows_reject_inexact_period() {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: Some("read_heavy_zipfian".to_string()),
			operation_mix: None,
			operation_schedule: None,
			operation_mix_period: 999,
		};

		let err = config.workloads().unwrap_err();

		assert!(err.to_string().contains("multiple of 20"));
	}

	#[test]
	fn sustained_ingest_rejects_non_create_custom_mix() -> Result<()> {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: Some("sustained_ingest".to_string()),
			operation_mix: Some("read=1.0".to_string()),
			operation_schedule: Some(parse_operation_mix("read=1.0", 10)?),
			operation_mix_period: 10,
		};

		let err = config.workloads().expect_err("sustained ingest rejects reads");

		assert!(err.to_string().contains("incompatible with sustained_ingest"));
		Ok(())
	}

	#[test]
	fn zipfian_rows_reject_scan_custom_mix() -> Result<()> {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: Some("balanced_zipfian".to_string()),
			operation_mix: Some("scan=1.0".to_string()),
			operation_schedule: Some(parse_operation_mix("scan=1.0", 10)?),
			operation_mix_period: 10,
		};

		let err = config.workloads().expect_err("zipfian rows reject scans");

		assert!(err.to_string().contains("incompatible with balanced_zipfian"));
		Ok(())
	}

	#[test]
	fn prepared_workload_rejects_custom_create_mix() -> Result<()> {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::ZERO,
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: Some("balanced_zipfian".to_string()),
			operation_mix: Some("create=1.0".to_string()),
			operation_schedule: Some(parse_operation_mix("create=1.0", 10)?),
			operation_mix_period: 10,
		};

		let err = config.workloads().expect_err("prepared workloads reject creates");

		assert!(err.to_string().contains("may include create only for sustained_ingest"));
		Ok(())
	}

	#[test]
	fn sustained_ingest_offsets_create_keys_after_warmup() {
		let config = SteadyStateConfig {
			records: 100,
			warmup: Duration::from_secs(1),
			measurement: Duration::from_secs(1),
			latency_sample_every: 1,
			seed: 1,
			zipfian_exponent: 0.99,
			bench_spec: Some("sustained_ingest".to_string()),
			operation_mix: None,
			operation_schedule: None,
			operation_mix_period: 1000,
		};
		assert!(matches!(
			SteadyStateWorkload::SustainedIngest.operation_at(&config, 0),
			SteadyStateOperation::Create
		));
		assert_eq!(
			SteadyStateWorkload::SustainedIngest.expected_mix_prefix(&config, 5),
			"create=5"
		);
	}
}

/// Single logical workload dispatched to [`BenchmarkClient`] (CRUD, scan, index, or batch).
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum BenchmarkOperation {
	/// Insert new keys up to the sample count.
	Create,
	/// Read by key.
	Read,
	/// Update existing keys.
	Update,
	/// Table or indexed query for a [`Scan`] and [`ScanContext`].
	Scan(Scan, ScanContext),
	/// KNN query against a pre-fetched holdout query set; only the call into
	/// the engine is timed (the read used to materialise the query lives in
	/// the holdout setup, not in this window).
	VectorScan(Scan, ScanContext, VectorQuerySet),
	/// Scan plus mixed writes according to [`ScanWithWrites`].
	ScanWithWrites(Scan, ScanContext, ScanWithWrites),
	/// Create backing index for the given analyzer/index id, tagged with the
	/// scan run name so two BuildIndex calls under the same scan id (e.g. the
	/// `count` vs `select` query shapes of the same field group) are
	/// distinguishable in phase markers and per-phase perf files.
	BuildIndex(Index, String, String),
	/// Create a vector index (HNSW / DiskANN) carrying the algorithm-specific knobs.
	BuildVectorIndex(Index, VectorQuerySpec, usize, String),
	/// Drop index by stable scan id, tagged with the scan run name for the
	/// same reason as [`BuildIndex`].
	RemoveIndex(String, String),
	/// Delete by key.
	Delete,
	/// Batch insert configured by [`BatchOperation`].
	BatchCreate(BatchOperation),
	/// Batch read by keys from [`BatchOperation`].
	BatchRead(BatchOperation),
	/// Batch update configured by [`BatchOperation`].
	BatchUpdate(BatchOperation),
	/// Batch delete configured by [`BatchOperation`].
	BatchDelete(BatchOperation),
}

/// Short slug for UI labels: heap scan vs index-backed scan.
fn scan_context_slug(ctx: ScanContext) -> &'static str {
	match ctx {
		ScanContext::WithoutIndex => "no-index",
		ScanContext::WithIndex => "indexed",
	}
}

impl Display for BenchmarkOperation {
	/// Human-readable phase name for logs and progress bars.
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Create => write!(f, "Create"),
			Self::Read => write!(f, "Read"),
			Self::Scan(_, ctx) => {
				write!(f, "Scan :: {}", scan_context_slug(*ctx))
			}
			Self::VectorScan(_, ctx, _) => {
				write!(f, "VectorScan :: {}", scan_context_slug(*ctx))
			}
			Self::BuildVectorIndex(_, _, _, _) => write!(f, "BuildVectorIndex"),
			Self::ScanWithWrites(_, ctx, spec) => {
				write!(
					f,
					"Scan :: {}, combined workload (ratio {}%)",
					scan_context_slug(*ctx),
					writes_ratio_percent(spec)
				)
			}
			Self::BuildIndex(_, _, _) => write!(f, "BuildIndex"),
			Self::RemoveIndex(_, _) => write!(f, "RemoveIndex"),
			Self::Update => write!(f, "Update"),
			Self::Delete => write!(f, "Delete"),
			Self::BatchCreate(b) => write!(f, "BatchCreate::{}", b.name),
			Self::BatchRead(b) => write!(f, "BatchRead::{}", b.name),
			Self::BatchUpdate(b) => write!(f, "BatchUpdate::{}", b.name),
			Self::BatchDelete(b) => write!(f, "BatchDelete::{}", b.name),
		}
	}
}

/// Grep-friendly marker label used in `--emit-phase-markers` lines.
///
/// `Display` collapses every scan onto `Scan :: <ctx>` and every BuildIndex /
/// RemoveIndex onto the bare op name, which is fine for the human-readable UI
/// but means dev.sh's profiling loop can't tell adjacent runs apart. This
/// helper expands the label with the scan id (and run name for plain scans)
/// so each marker line is unique within a benchmark run.
fn phase_marker_label(op: &BenchmarkOperation) -> String {
	match op {
		BenchmarkOperation::Scan(scan, ctx) => {
			format!("Scan :: {} :: {} :: {}", scan.id, scan.name, scan_context_slug(*ctx))
		}
		BenchmarkOperation::ScanWithWrites(scan, ctx, spec) => {
			format!(
				"Scan :: {} :: {} :: {}, writes {}%",
				scan.id,
				scan.name,
				scan_context_slug(*ctx),
				writes_ratio_percent(spec)
			)
		}
		BenchmarkOperation::BuildIndex(_, scan_id, scan_name) => {
			format!("BuildIndex :: {scan_id} :: {scan_name}")
		}
		BenchmarkOperation::RemoveIndex(scan_id, scan_name) => {
			format!("RemoveIndex :: {scan_id} :: {scan_name}")
		}
		_ => op.to_string(),
	}
}

/// Truncated label for the indicatif progress bar (scan/batch variants).
fn progress_short_label(operation: &BenchmarkOperation) -> String {
	const MAX: usize = 72;
	let s = match operation {
		BenchmarkOperation::Scan(_, ctx) => scan_context_slug(*ctx).to_string(),
		BenchmarkOperation::VectorScan(_, ctx, _) => {
			format!("vector knn :: {}", scan_context_slug(*ctx))
		}
		BenchmarkOperation::ScanWithWrites(_, ctx, spec) => {
			format!("{}, writes {}%", scan_context_slug(*ctx), writes_ratio_percent(spec))
		}
		BenchmarkOperation::BuildIndex(_, _, _) => "BuildIndex".to_string(),
		BenchmarkOperation::BuildVectorIndex(_, _, _, _) => "BuildVectorIndex".to_string(),
		BenchmarkOperation::RemoveIndex(_, _) => "RemoveIndex".to_string(),
		_ => operation.to_string(),
	};
	if s.len() > MAX {
		format!("{}…", &s[..MAX.saturating_sub(1)])
	} else {
		s
	}
}
