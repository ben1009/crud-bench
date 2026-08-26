use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use serde::Deserialize;
use serde_json::Value;

const DEFAULT_ROWS: &[&str] =
	&["put_c", "batch_create_100", "batch_create_1000", "batch_delete_100", "batch_delete_1000"];
const DEFAULT_RATIO_ROWS: &[&str] = &["put_c", "batch_create_1000", "batch_delete_1000"];
const DEFAULT_STEADY_STATE_ROWS: &[&str] = &[
	"balanced_zipfian",
	"read_heavy_zipfian",
	"update_heavy_zipfian",
	"point_read_zipfian",
	"point_read_uniform",
	"point_read_missing_in_range",
	"range_scan_uniform",
	"sustained_ingest",
];

#[derive(Parser, Debug)]
#[command(name = "perf-gate")]
#[command(about = "Check crud-bench sync performance gates for ToyKV artifacts")]
struct Args {
	/// Previous ToyKV --sync crud-bench CSV.
	#[arg(long)]
	baseline_sync: Option<PathBuf>,
	/// Current ToyKV --sync crud-bench CSV.
	#[arg(long)]
	current_sync: Option<PathBuf>,
	/// Previous ToyKV no-sync crud-bench CSV.
	#[arg(long)]
	baseline_nosync: Option<PathBuf>,
	/// Current ToyKV no-sync crud-bench CSV.
	#[arg(long)]
	current_nosync: Option<PathBuf>,
	/// Previous steady-state JSON artifact.
	#[arg(long)]
	baseline_steady_state_json: Option<PathBuf>,
	/// Current steady-state JSON artifact.
	#[arg(long)]
	current_steady_state_json: Option<PathBuf>,
	/// Required steady-state row. Defaults to the RFC MVP rows in steady-state mode.
	#[arg(long = "steady-state-row")]
	steady_state_rows: Vec<String>,
	/// Optional steady-state row; unsupported rows with these names are skipped.
	#[arg(long = "optional-steady-state-row")]
	optional_steady_state_rows: Vec<String>,
	/// Current Fjall --sync crud-bench CSV. When present, ToyKV must stay at or above Fjall on
	/// gated rows.
	#[arg(long)]
	fjall_sync: Option<PathBuf>,
	/// Previous single-client ToyKV --sync CSV for latency checks.
	#[arg(long, requires = "current_latency_sync")]
	baseline_latency_sync: Option<PathBuf>,
	/// Current single-client ToyKV --sync CSV for latency checks.
	#[arg(long, requires = "baseline_latency_sync")]
	current_latency_sync: Option<PathBuf>,
	/// Rows to gate. Uses stable aliases such as put_c and batch_create_1000.
	#[arg(long = "row")]
	rows: Vec<String>,
	/// Rows where sync/no-sync ratio must improve. Defaults to put_c, batch_create_1000,
	/// batch_delete_1000.
	#[arg(long = "ratio-row")]
	ratio_rows: Vec<String>,
	/// Maximum allowed current-sync OPS regression versus baseline sync.
	#[arg(long, default_value_t = 5.0)]
	max_sync_regression_pct: f64,
	/// Maximum allowed steady-state OPS regression versus the baseline artifact.
	#[arg(long, default_value_t = 5.0)]
	max_ops_regression_pct: f64,
	/// Allow baseline and current steady-state artifacts to represent different backends.
	#[arg(long)]
	allow_database_mismatch: bool,
	/// Minimum number of ratio rows that must improve.
	#[arg(long, default_value_t = 2)]
	min_ratio_improvements: usize,
	/// Maximum allowed p95/p99 latency regression when latency CSVs are supplied.
	#[arg(long, default_value_t = 5.0)]
	max_latency_regression_pct: f64,
}

#[derive(Clone, Debug)]
struct BenchRow {
	ops: f64,
	p95_ms: f64,
	p99_ms: f64,
}

#[derive(Debug)]
struct GateConfig {
	rows: Vec<String>,
	ratio_rows: Vec<String>,
	max_sync_regression_pct: f64,
	min_ratio_improvements: usize,
	max_latency_regression_pct: f64,
}

#[derive(Debug)]
struct GateInputs {
	baseline_sync: BenchCsv,
	current_sync: BenchCsv,
	baseline_nosync: BenchCsv,
	current_nosync: BenchCsv,
	fjall_sync: Option<BenchCsv>,
	baseline_latency_sync: Option<BenchCsv>,
	current_latency_sync: Option<BenchCsv>,
}

#[derive(Debug)]
struct SteadyStateGateConfig {
	rows: Vec<String>,
	optional_rows: Vec<String>,
	max_ops_regression_pct: f64,
	max_latency_regression_pct: f64,
	allow_database_mismatch: bool,
}

#[derive(Debug)]
struct SteadyStateGateInputs {
	baseline: SteadyStateRows,
	current: SteadyStateRows,
}

struct Evaluation {
	report: String,
	passed: bool,
}

type BenchCsv = HashMap<String, BenchRow>;
type SteadyStateRows = HashMap<String, SteadyStateRow>;

#[derive(Clone, Debug, Deserialize)]
struct SteadyStateArtifact {
	#[serde(default)]
	steady_state: Vec<SteadyStateRow>,
}

#[derive(Clone, Debug, Deserialize)]
struct SteadyStateRow {
	name: String,
	suite: String,
	database: serde_json::Value,
	status: String,
	#[serde(default)]
	unsupported_reason: Option<String>,
	#[serde(default)]
	failure_reason: Option<String>,
	sync: bool,
	task: SteadyStateTask,
	phases: SteadyStatePhases,
	#[serde(default)]
	throughput: Option<SteadyStateThroughput>,
	#[serde(default)]
	latency: Option<SteadyStateLatency>,
	#[serde(default)]
	validation: Option<SteadyStateValidation>,
	#[serde(default)]
	drain: Option<SteadyStateDrain>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct SteadyStateTask {
	records: u32,
	clients: u32,
	threads: u32,
	warmup_secs: u64,
	measurement_secs: u64,
	latency_sample_every: u64,
	seed: u64,
	worker_seed_derivation: String,
	operation_mix: String,
	operation_mix_period: u32,
	key_selection: String,
	zipfian_exponent: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct SteadyStatePhases {
	prepare: SteadyStatePhase,
	warmup: SteadyStatePhase,
	measure: SteadyStatePhase,
	drain: SteadyStatePhase,
	cleanup: SteadyStatePhase,
}

#[derive(Clone, Debug, Deserialize)]
struct SteadyStatePhase {
	elapsed_ms: f64,
	status: String,
}

#[derive(Clone, Debug, Deserialize)]
struct SteadyStateThroughput {
	completed_operations: u64,
	ops_per_sec: f64,
	#[serde(default)]
	per_second_windows: Vec<f64>,
}

#[derive(Clone, Debug, Deserialize)]
struct SteadyStateLatency {
	sample_count: u64,
	p50_ms: f64,
	p95_ms: f64,
	p99_ms: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct SteadyStateValidation {
	errors: u64,
	read_hits: u64,
	read_misses: u64,
	updates: u64,
	scan_count_errors: u64,
	observed_mix: String,
	expected_mix_prefix: String,
}

#[derive(Clone, Debug, Deserialize)]
struct SteadyStateDrain {
	elapsed_ms: f64,
	timed_out: bool,
}

fn main() -> Result<ExitCode> {
	let args = Args::parse();
	let steady_state_mode = uses_steady_state_mode(&args);
	if steady_state_mode {
		let baseline = args
			.baseline_steady_state_json
			.as_deref()
			.context("--baseline-steady-state-json is required for steady-state mode")?;
		let current = args
			.current_steady_state_json
			.as_deref()
			.context("--current-steady-state-json is required for steady-state mode")?;
		if args.baseline_sync.is_some()
			|| args.current_sync.is_some()
			|| args.baseline_nosync.is_some()
			|| args.current_nosync.is_some()
			|| args.fjall_sync.is_some()
			|| args.baseline_latency_sync.is_some()
			|| args.current_latency_sync.is_some()
			|| !args.rows.is_empty()
			|| !args.ratio_rows.is_empty()
		{
			bail!("steady-state mode cannot be combined with legacy CRUD CSV inputs");
		}
		let rows =
			steady_state_rows_to_evaluate(args.steady_state_rows, &args.optional_steady_state_rows);
		let cfg = SteadyStateGateConfig {
			rows,
			optional_rows: args.optional_steady_state_rows,
			max_ops_regression_pct: args.max_ops_regression_pct,
			max_latency_regression_pct: args.max_latency_regression_pct,
			allow_database_mismatch: args.allow_database_mismatch,
		};
		let inputs = SteadyStateGateInputs {
			baseline: read_steady_state_json(baseline)?,
			current: read_steady_state_json(current)?,
		};
		let eval = evaluate_steady_state(&cfg, &inputs)?;
		print!("{}", eval.report);
		return Ok(if eval.passed {
			ExitCode::SUCCESS
		} else {
			ExitCode::FAILURE
		});
	}

	let rows = if args.rows.is_empty() {
		DEFAULT_ROWS.iter().map(|row| row.to_string()).collect()
	} else {
		args.rows
	};
	let ratio_rows = if args.ratio_rows.is_empty() {
		DEFAULT_RATIO_ROWS.iter().map(|row| row.to_string()).collect()
	} else {
		args.ratio_rows
	};
	let cfg = GateConfig {
		rows,
		ratio_rows,
		max_sync_regression_pct: args.max_sync_regression_pct,
		min_ratio_improvements: args.min_ratio_improvements,
		max_latency_regression_pct: args.max_latency_regression_pct,
	};
	validate_config(&cfg)?;
	let inputs = GateInputs {
		baseline_sync: read_crud_bench_csv(
			args.baseline_sync.as_deref().context("--baseline-sync is required")?,
		)?,
		current_sync: read_crud_bench_csv(
			args.current_sync.as_deref().context("--current-sync is required")?,
		)?,
		baseline_nosync: read_crud_bench_csv(
			args.baseline_nosync.as_deref().context("--baseline-nosync is required")?,
		)?,
		current_nosync: read_crud_bench_csv(
			args.current_nosync.as_deref().context("--current-nosync is required")?,
		)?,
		fjall_sync: args.fjall_sync.as_deref().map(read_crud_bench_csv).transpose()?,
		baseline_latency_sync: args
			.baseline_latency_sync
			.as_deref()
			.map(read_crud_bench_csv)
			.transpose()?,
		current_latency_sync: args
			.current_latency_sync
			.as_deref()
			.map(read_crud_bench_csv)
			.transpose()?,
	};

	let eval = evaluate(&cfg, &inputs)?;
	print!("{}", eval.report);
	if !eval.passed {
		return Ok(ExitCode::FAILURE);
	}
	Ok(ExitCode::SUCCESS)
}

fn uses_steady_state_mode(args: &Args) -> bool {
	args.baseline_steady_state_json.is_some()
		|| args.current_steady_state_json.is_some()
		|| !args.steady_state_rows.is_empty()
		|| !args.optional_steady_state_rows.is_empty()
		|| args.allow_database_mismatch
}

fn steady_state_rows_to_evaluate(
	required_rows: Vec<String>,
	optional_rows: &[String],
) -> Vec<String> {
	let mut rows: Vec<_> = if required_rows.is_empty() {
		DEFAULT_STEADY_STATE_ROWS.iter().map(|row| row.to_string()).collect()
	} else {
		required_rows
	};
	for row in optional_rows {
		if !rows.contains(row) {
			rows.push(row.clone());
		}
	}
	rows
}

fn read_crud_bench_csv(path: &Path) -> Result<BenchCsv> {
	let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
	parse_crud_bench_csv(file).with_context(|| format!("failed to parse {}", path.display()))
}

fn read_steady_state_json(path: &Path) -> Result<SteadyStateRows> {
	let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
	parse_steady_state_json(file).with_context(|| format!("failed to parse {}", path.display()))
}

fn parse_steady_state_json<R: Read>(reader: R) -> Result<SteadyStateRows> {
	let artifact: SteadyStateArtifact = serde_json::from_reader(reader)?;
	let mut rows = HashMap::new();
	for row in artifact.steady_state {
		let name = row.name.clone();
		if rows.insert(name.clone(), row).is_some() {
			bail!("duplicate steady-state row {name:?} found");
		}
	}
	Ok(rows)
}

fn parse_crud_bench_csv<R: Read>(reader: R) -> Result<BenchCsv> {
	let mut reader = csv::Reader::from_reader(reader);
	let header = reader.headers().context("missing CSV header")?.clone();
	let test_idx = column_index(&header, "Test")?;
	let ops_idx = column_index(&header, "OPS")?;
	let p99_idx = column_index(&header, "99th")?;
	let p95_idx = column_index(&header, "95th")?;
	let mut rows = HashMap::new();

	for (line_no, result) in reader.records().enumerate() {
		let record = result?;
		if record.len() <= test_idx
			|| record.len() <= ops_idx
			|| record.len() <= p95_idx
			|| record.len() <= p99_idx
		{
			bail!("CSV row {} has too few columns", line_no + 2);
		}
		if record[ops_idx].trim() == "-" {
			continue;
		}
		let label = record[test_idx].trim().to_string();
		let row = BenchRow {
			ops: parse_number(&record[ops_idx], "OPS")?,
			p95_ms: parse_duration_ms(&record[p95_idx])?,
			p99_ms: parse_duration_ms(&record[p99_idx])?,
		};
		if rows.insert(label.clone(), row).is_some() {
			bail!("duplicate row {label:?} found in CSV");
		}
	}

	Ok(rows)
}

fn column_index(header: &csv::StringRecord, name: &str) -> Result<usize> {
	header.iter().position(|col| col == name).ok_or_else(|| anyhow!("missing CSV column {name:?}"))
}

fn parse_number(cell: &str, label: &str) -> Result<f64> {
	let value =
		cell.trim().parse::<f64>().with_context(|| format!("invalid {label} value {cell:?}"))?;
	if !value.is_finite() || value < 0.0 {
		bail!("invalid {label} value {cell:?}: must be a non-negative number");
	}
	Ok(value)
}

fn parse_duration_ms(cell: &str) -> Result<f64> {
	let trimmed = cell.trim();
	if trimmed == "-" {
		return Ok(0.0);
	}
	let Some(value) = trimmed.strip_suffix("ms") else {
		bail!("expected duration in ms, got {cell:?}");
	};
	parse_number(value.trim(), "duration")
}

fn row_alias(label: &str) -> String {
	let label = label.trim();
	if label == "[C]reate" {
		return "put_c".to_string();
	}
	if label == "[R]eads" || label == "[R]ead" {
		return "get_c".to_string();
	}
	if label == "[U]pdate" {
		return "update_c".to_string();
	}
	if label == "[D]elete" {
		return "delete_c".to_string();
	}
	if let Some(rest) = label.strip_prefix("[B]atch::") {
		let name = rest.split_whitespace().next().unwrap_or(rest);
		return name.to_string();
	}
	label.to_string()
}

fn evaluate(cfg: &GateConfig, inputs: &GateInputs) -> Result<Evaluation> {
	validate_config(cfg)?;

	let mut failures = Vec::new();
	let mut output = String::from("ToyKV sync perf gate\n\n");
	output.push_str("Sync OPS regression gate:\n");

	for row in &cfg.rows {
		let baseline = required_row(&inputs.baseline_sync, row, "baseline sync")?;
		let current = required_row(&inputs.current_sync, row, "current sync")?;
		let delta = percent_change(current.ops, baseline.ops);
		output.push_str(&format!(
			"- {row}: {:.2} -> {:.2} OPS ({:+.2}%)\n",
			baseline.ops, current.ops, delta
		));
		if delta < -cfg.max_sync_regression_pct {
			failures.push(format!(
				"{row} sync OPS regressed {delta:.2}%, below -{:.2}%",
				cfg.max_sync_regression_pct
			));
		}
	}

	output.push_str("\nSync/no-sync ratio gate:\n");
	let mut improved = 0usize;
	for row in &cfg.ratio_rows {
		let baseline_sync = required_row(&inputs.baseline_sync, row, "baseline sync")?;
		let current_sync = required_row(&inputs.current_sync, row, "current sync")?;
		let baseline_nosync = required_row(&inputs.baseline_nosync, row, "baseline no-sync")?;
		let current_nosync = required_row(&inputs.current_nosync, row, "current no-sync")?;
		let baseline_ratio = ratio(baseline_sync.ops, baseline_nosync.ops)?;
		let current_ratio = ratio(current_sync.ops, current_nosync.ops)?;
		let delta = percent_change(current_ratio, baseline_ratio);
		if current_ratio > baseline_ratio {
			improved += 1;
		}
		output.push_str(&format!(
			"- {row}: {:.2}% -> {:.2}% ({:+.2}%)\n",
			baseline_ratio * 100.0,
			current_ratio * 100.0,
			delta
		));
	}
	if improved < cfg.min_ratio_improvements {
		failures.push(format!(
			"only {improved} sync/no-sync ratio rows improved; need {}",
			cfg.min_ratio_improvements
		));
	}

	if let Some(fjall) = &inputs.fjall_sync {
		output.push_str("\nFjall-relative sync OPS gate:\n");
		for row in &cfg.rows {
			let current = required_row(&inputs.current_sync, row, "current sync")?;
			let fjall_row = required_row(fjall, row, "Fjall sync")?;
			let delta = percent_change(current.ops, fjall_row.ops);
			output.push_str(&format!(
				"- {row}: ToyKV {:.2} / Fjall {:.2} OPS ({:+.2}%)\n",
				current.ops, fjall_row.ops, delta
			));
			if delta < -cfg.max_sync_regression_pct {
				failures.push(format!(
					"{row} current sync OPS is below Fjall by {delta:.2}%, below -{:.2}%",
					cfg.max_sync_regression_pct
				));
			}
		}
	}

	match (&inputs.baseline_latency_sync, &inputs.current_latency_sync) {
		(Some(baseline), Some(current)) => {
			output.push_str("\nSingle-client p95/p99 latency gate:\n");
			for row in &cfg.rows {
				let baseline = required_row(baseline, row, "baseline latency sync")?;
				let current = required_row(current, row, "current latency sync")?;
				let p95_delta = percent_change(current.p95_ms, baseline.p95_ms);
				let p99_delta = percent_change(current.p99_ms, baseline.p99_ms);
				output.push_str(&format!(
					"- {row}: p95 {:.2} -> {:.2} ms ({:+.2}%), p99 {:.2} -> {:.2} ms ({:+.2}%)\n",
					baseline.p95_ms,
					current.p95_ms,
					p95_delta,
					baseline.p99_ms,
					current.p99_ms,
					p99_delta
				));
				if p95_delta > cfg.max_latency_regression_pct {
					failures.push(format!(
						"{row} p95 regressed {p95_delta:.2}%, above {:.2}%",
						cfg.max_latency_regression_pct
					));
				}
				if p99_delta > cfg.max_latency_regression_pct {
					failures.push(format!(
						"{row} p99 regressed {p99_delta:.2}%, above {:.2}%",
						cfg.max_latency_regression_pct
					));
				}
			}
		}
		(None, None) => {
			output.push_str(
				"\nSingle-client p95/p99 latency gate: skipped; no latency CSVs supplied.\n",
			);
		}
		_ => bail!("latency gate requires both --baseline-latency-sync and --current-latency-sync"),
	}

	if failures.is_empty() {
		output.push_str("\nResult: PASS\n");
		Ok(Evaluation {
			report: output,
			passed: true,
		})
	} else {
		output.push_str("\nResult: FAIL\n");
		for failure in &failures {
			output.push_str(&format!("- {failure}\n"));
		}
		Ok(Evaluation {
			report: output,
			passed: false,
		})
	}
}

fn evaluate_steady_state(
	cfg: &SteadyStateGateConfig,
	inputs: &SteadyStateGateInputs,
) -> Result<Evaluation> {
	validate_steady_state_config(cfg)?;

	let mut failures = Vec::new();
	let mut output = String::from("ToyKV steady-state perf gate\n\n");
	output.push_str("Steady-state row gate:\n");

	for row_name in &cfg.rows {
		let baseline = required_steady_state_row(&inputs.baseline, row_name, "baseline")?;
		let current = required_steady_state_row(&inputs.current, row_name, "current")?;
		validate_steady_state_row(row_name, baseline, "baseline", cfg, &mut failures);
		validate_steady_state_row(row_name, current, "current", cfg, &mut failures);
		let baseline_optional_unsupported =
			is_optional_unsupported(row_name, &baseline.status, cfg);
		let current_optional_unsupported = is_optional_unsupported(row_name, &current.status, cfg);
		validate_steady_state_comparable(
			row_name,
			baseline,
			current,
			cfg.allow_database_mismatch,
			&mut failures,
		);
		if row_name == "idle" {
			output.push_str("- idle: no client operations; phase timings only\n");
			continue;
		}

		if baseline.status == "completed" && current.status == "completed" {
			let Some(baseline_throughput) = &baseline.throughput else {
				continue;
			};
			let Some(current_throughput) = &current.throughput else {
				continue;
			};
			let Some(baseline_latency) = &baseline.latency else {
				continue;
			};
			let Some(current_latency) = &current.latency else {
				continue;
			};
			let ops_delta =
				percent_change(current_throughput.ops_per_sec, baseline_throughput.ops_per_sec);
			let p95_delta = percent_change(current_latency.p95_ms, baseline_latency.p95_ms);
			let p99_delta = percent_change(current_latency.p99_ms, baseline_latency.p99_ms);
			output.push_str(&format!(
				"- {row_name}: {} OPS {:.2} -> {} OPS {:.2} ({:+.2}%), {} p95 {:.2} -> {} p95 {:.2} ms ({:+.2}%), {} p99 {:.2} -> {} p99 {:.2} ms ({:+.2}%)\n",
				database_label(&baseline.database),
				baseline_throughput.ops_per_sec,
				database_label(&current.database),
				current_throughput.ops_per_sec,
				ops_delta,
				database_label(&baseline.database),
				baseline_latency.p95_ms,
				database_label(&current.database),
				current_latency.p95_ms,
				p95_delta,
				database_label(&baseline.database),
				baseline_latency.p99_ms,
				database_label(&current.database),
				current_latency.p99_ms,
				p99_delta
			));
			if ops_delta < -cfg.max_ops_regression_pct {
				failures.push(format!(
					"{row_name} OPS regressed {ops_delta:.2}%, below -{:.2}%",
					cfg.max_ops_regression_pct
				));
			}
			if p95_delta > cfg.max_latency_regression_pct {
				failures.push(format!(
					"{row_name} p95 regressed {p95_delta:.2}%, above {:.2}%",
					cfg.max_latency_regression_pct
				));
			}
			if p99_delta > cfg.max_latency_regression_pct {
				failures.push(format!(
					"{row_name} p99 regressed {p99_delta:.2}%, above {:.2}%",
					cfg.max_latency_regression_pct
				));
			}
		} else if baseline_optional_unsupported && current_optional_unsupported {
			output.push_str(&format!("- {row_name}: optional unsupported; skipped\n"));
		} else {
			output.push_str(&format!(
				"- {row_name}: baseline status={}, current status={}\n",
				baseline.status, current.status
			));
			if baseline_optional_unsupported != current_optional_unsupported {
				failures.push(format!(
					"{row_name} optional unsupported state differs: baseline={}, current={}",
					baseline.status, current.status
				));
			}
		}
	}

	if failures.is_empty() {
		output.push_str("\nResult: PASS\n");
		Ok(Evaluation {
			report: output,
			passed: true,
		})
	} else {
		output.push_str("\nResult: FAIL\n");
		for failure in &failures {
			output.push_str(&format!("- {failure}\n"));
		}
		Ok(Evaluation {
			report: output,
			passed: false,
		})
	}
}

fn database_label(database: &Value) -> &str {
	database.as_str().unwrap_or("unknown")
}

fn validate_steady_state_row(
	row_name: &str,
	row: &SteadyStateRow,
	source: &str,
	cfg: &SteadyStateGateConfig,
	failures: &mut Vec<String>,
) {
	if row.suite != "steady-state" {
		failures.push(format!("{source} {row_name} has invalid suite {:?}", row.suite));
	}
	if row.database.as_str().is_none_or(|database| database.trim().is_empty()) {
		failures.push(format!("{source} {row_name} has invalid database field"));
	}
	validate_steady_state_task(row_name, &row.task, source, failures);
	validate_steady_state_phases(row_name, row, source, failures);
	match row.status.as_str() {
		"completed" => {}
		"unsupported" if cfg.optional_rows.iter().any(|optional| optional == row_name) => {
			if row.unsupported_reason.as_deref().is_none_or(str::is_empty) {
				failures.push(format!("{source} {row_name} is unsupported without a reason"));
			}
			return;
		}
		"unsupported" => {
			failures.push(format!(
				"{source} {row_name} is unsupported: {}",
				row.unsupported_reason.as_deref().unwrap_or("missing reason")
			));
			return;
		}
		"failed" => {
			failures.push(format!(
				"{source} {row_name} failed: {}",
				row.failure_reason.as_deref().unwrap_or("missing reason")
			));
			return;
		}
		other => {
			failures.push(format!("{source} {row_name} has unknown status {other:?}"));
			return;
		}
	}
	if row_name == "idle" {
		let Some(validation) = &row.validation else {
			failures.push(format!("{source} {row_name} is missing validation"));
			return;
		};
		if validation.errors > 0 {
			failures
				.push(format!("{source} {row_name} has {} validation errors", validation.errors));
		}
		if validation.read_hits != 0
			|| validation.read_misses != 0
			|| validation.updates != 0
			|| validation.scan_count_errors != 0
		{
			failures.push(format!("{source} {row_name} reports client operations"));
		}
		if validation.observed_mix != "none" || validation.expected_mix_prefix != "none" {
			failures.push(format!("{source} {row_name} has invalid idle operation mix"));
		}
		if row.throughput.is_some() || row.latency.is_some() {
			failures.push(format!("{source} {row_name} must not report throughput or latency"));
		}
		let Some(drain) = &row.drain else {
			failures.push(format!("{source} {row_name} is missing drain"));
			return;
		};
		if drain.elapsed_ms < 0.0 || !drain.elapsed_ms.is_finite() || drain.timed_out {
			failures.push(format!("{source} {row_name} has invalid drain"));
		}
		return;
	}
	let Some(throughput) = &row.throughput else {
		failures.push(format!("{source} {row_name} is missing throughput"));
		return;
	};
	if throughput.completed_operations == 0 {
		failures.push(format!("{source} {row_name} completed zero operations"));
	}
	if throughput.ops_per_sec <= 0.0 || !throughput.ops_per_sec.is_finite() {
		failures.push(format!("{source} {row_name} has invalid OPS"));
	}
	if throughput.per_second_windows.is_empty() {
		failures.push(format!("{source} {row_name} is missing throughput windows"));
	}
	if throughput.per_second_windows.iter().any(|window| *window < 0.0 || !window.is_finite()) {
		failures.push(format!("{source} {row_name} has invalid throughput windows"));
	}
	let Some(validation) = &row.validation else {
		failures.push(format!("{source} {row_name} is missing validation"));
		return;
	};
	if validation.errors > 0 {
		failures.push(format!("{source} {row_name} has {} validation errors", validation.errors));
	}
	if validation.observed_mix.is_empty() || validation.expected_mix_prefix.is_empty() {
		failures.push(format!("{source} {row_name} is missing validation mix details"));
	}
	let _ = (
		validation.read_hits,
		validation.read_misses,
		validation.updates,
		validation.scan_count_errors,
	);
	let Some(latency) = &row.latency else {
		failures.push(format!("{source} {row_name} is missing latency"));
		return;
	};
	if row.task.latency_sample_every > 0 && latency.sample_count == 0 {
		failures.push(format!("{source} {row_name} is missing latency samples"));
	}
	if latency.p50_ms < 0.0
		|| latency.p95_ms < 0.0
		|| latency.p99_ms < 0.0
		|| !latency.p50_ms.is_finite()
		|| !latency.p95_ms.is_finite()
		|| !latency.p99_ms.is_finite()
	{
		failures.push(format!("{source} {row_name} has invalid latency"));
	}
	if latency.p50_ms > latency.p95_ms || latency.p95_ms > latency.p99_ms {
		failures.push(format!("{source} {row_name} has unordered latency quantiles"));
	}
	let Some(drain) = &row.drain else {
		failures.push(format!("{source} {row_name} is missing drain"));
		return;
	};
	if drain.elapsed_ms < 0.0 || !drain.elapsed_ms.is_finite() {
		failures.push(format!("{source} {row_name} has invalid drain elapsed time"));
	}
	if drain.timed_out {
		failures.push(format!("{source} {row_name} drain timed out"));
	}
}

fn validate_steady_state_task(
	row_name: &str,
	task: &SteadyStateTask,
	source: &str,
	failures: &mut Vec<String>,
) {
	if task.records == 0 {
		failures.push(format!("{source} {row_name} has zero records"));
	}
	if task.clients == 0 || task.threads == 0 {
		failures.push(format!("{source} {row_name} has invalid concurrency"));
	}
	if task.measurement_secs == 0 {
		failures.push(format!("{source} {row_name} has zero measurement duration"));
	}
	if task.worker_seed_derivation.is_empty()
		|| task.operation_mix.is_empty()
		|| task.key_selection.is_empty()
	{
		failures.push(format!("{source} {row_name} is missing task metadata"));
	}
	if task.operation_mix_period == 0 {
		failures.push(format!("{source} {row_name} has zero operation mix period"));
	}
	if task.zipfian_exponent < 0.0 || !task.zipfian_exponent.is_finite() {
		failures.push(format!("{source} {row_name} has invalid Zipfian exponent"));
	}
	let _ = (task.warmup_secs, task.latency_sample_every, task.seed);
}

fn validate_steady_state_comparable(
	row_name: &str,
	baseline: &SteadyStateRow,
	current: &SteadyStateRow,
	allow_database_mismatch: bool,
	failures: &mut Vec<String>,
) {
	if !allow_database_mismatch && baseline.database != current.database {
		failures.push(format!("{row_name} database differs between baseline and current"));
	}
	if baseline.sync != current.sync {
		failures.push(format!("{row_name} sync setting differs between baseline and current"));
	}
	if baseline.task != current.task {
		failures.push(format!("{row_name} task metadata differs between baseline and current"));
	}
}

fn validate_steady_state_phases(
	row_name: &str,
	row: &SteadyStateRow,
	source: &str,
	failures: &mut Vec<String>,
) {
	let phases = &row.phases;
	for (phase_name, phase) in [
		("prepare", &phases.prepare),
		("warmup", &phases.warmup),
		("measure", &phases.measure),
		("drain", &phases.drain),
		("cleanup", &phases.cleanup),
	] {
		if phase.elapsed_ms < 0.0 || !phase.elapsed_ms.is_finite() {
			failures.push(format!("{source} {row_name} has invalid {phase_name} phase time"));
		}
		if !matches!(phase.status.as_str(), "completed" | "unsupported" | "failed") {
			failures.push(format!(
				"{source} {row_name} has invalid {phase_name} phase status {:?}",
				phase.status
			));
		}
		if row.status == "completed" && phase.status != "completed" {
			failures.push(format!(
				"{source} {row_name} completed row has non-completed {phase_name} phase"
			));
		}
	}
}

fn validate_config(cfg: &GateConfig) -> Result<()> {
	for row in cfg.rows.iter().chain(cfg.ratio_rows.iter()) {
		if is_steady_state_row(row) {
			bail!(
				"steady-state row {row:?} requires a steady-state JSON or sidecar gate, not the legacy CSV-only perf-gate"
			);
		}
	}
	if cfg.max_sync_regression_pct < 0.0 {
		bail!("--max-sync-regression-pct cannot be negative");
	}
	if cfg.max_latency_regression_pct < 0.0 {
		bail!("--max-latency-regression-pct cannot be negative");
	}
	if cfg.min_ratio_improvements > cfg.ratio_rows.len() {
		bail!(
			"--min-ratio-improvements ({}) cannot be greater than the number of ratio rows ({})",
			cfg.min_ratio_improvements,
			cfg.ratio_rows.len()
		);
	}
	Ok(())
}

fn validate_steady_state_config(cfg: &SteadyStateGateConfig) -> Result<()> {
	if cfg.max_ops_regression_pct < 0.0 {
		bail!("--max-ops-regression-pct cannot be negative");
	}
	if cfg.max_latency_regression_pct < 0.0 {
		bail!("--max-latency-regression-pct cannot be negative");
	}
	if cfg.rows.is_empty() {
		bail!("at least one steady-state row is required");
	}
	Ok(())
}

fn is_optional_unsupported(row_name: &str, status: &str, cfg: &SteadyStateGateConfig) -> bool {
	status == "unsupported" && cfg.optional_rows.iter().any(|optional| optional == row_name)
}

fn is_steady_state_row(row: &str) -> bool {
	row == "steady-state"
		|| row.starts_with("[T]steady-state::")
		|| row.starts_with("steady-state::")
		|| matches!(
			row,
			"balanced_zipfian"
				| "read_heavy_zipfian"
				| "update_heavy_zipfian"
				| "point_read_zipfian"
				| "point_read_uniform"
				| "point_read_missing_in_range"
				| "range_scan_uniform"
				| "sustained_ingest"
				| "idle"
		)
}

fn required_steady_state_row<'a>(
	rows: &'a SteadyStateRows,
	row: &str,
	source: &str,
) -> Result<&'a SteadyStateRow> {
	rows.get(row).ok_or_else(|| anyhow!("missing steady-state row {row:?} in {source} JSON"))
}

fn required_row<'a>(rows: &'a BenchCsv, row: &str, source: &str) -> Result<&'a BenchRow> {
	if let Some(r) = rows.get(row) {
		return Ok(r);
	}

	let matches: Vec<_> = rows.iter().filter(|(label, _)| row_alias(label) == row).collect();
	match matches.as_slice() {
		[(_, r)] => Ok(r),
		[] => bail!("missing row {row:?} in {source} CSV"),
		_ => bail!("ambiguous row {row:?} in {source} CSV: multiple matches found"),
	}
}

fn ratio(numerator: f64, denominator: f64) -> Result<f64> {
	if denominator <= 0.0 {
		bail!("cannot compute ratio against non-positive no-sync OPS {denominator}");
	}
	Ok(numerator / denominator)
}

fn percent_change(current: f64, baseline: f64) -> f64 {
	if baseline == 0.0 {
		if current > 0.0 {
			return f64::INFINITY;
		}
		return 0.0;
	}
	(current - baseline) * 100.0 / baseline
}

#[cfg(test)]
mod tests {
	use super::*;

	const CSV: &str = "\
Test,Total time,Mean,Max,99th,95th,75th,50th,25th,1st,Min,IQR,OPS,CPU_avg,CPU_min,CPU_max,Memory_peak,Memory_avg,Reads,Writes,System load,System load (1m/5m/15m)
[C]reate,1s,1.00 ms,2.00 ms,1.90 ms,1.80 ms,1.50 ms,1.00 ms,0.50 ms,0.10 ms,0.01 ms,1.00 ms,1000.00,0,0,0,0,0,0,0,0,0/0/0
[B]atch::batch_create_1000 (100 batches of 1000),1s,1.00 ms,2.00 ms,1.90 ms,1.80 ms,1.50 ms,1.00 ms,0.50 ms,0.10 ms,0.01 ms,1.00 ms,500.00,0,0,0,0,0,0,0,0,0/0/0
";
	const STEADY_STATE_JSON: &str = r#"{
  "steady_state": [
    {
      "name": "balanced_zipfian",
      "suite": "steady-state",
      "database": "toykv",
      "status": "completed",
      "unsupported_reason": null,
      "sync": true,
      "task": {
        "records": 100,
        "clients": 1,
        "threads": 1,
        "warmup_secs": 0,
        "measurement_secs": 1,
        "latency_sample_every": 1,
        "seed": 1,
        "worker_seed_derivation": "splitmix64(seed ^ worker_index)",
        "operation_mix": "read=1.000000",
        "operation_mix_period": 1000,
        "key_selection": "scrambled_zipfian",
        "zipfian_exponent": 0.99
      },
      "phases": {
        "prepare": { "elapsed_ms": 1.0, "status": "completed" },
        "warmup": { "elapsed_ms": 0.0, "status": "completed" },
        "measure": { "elapsed_ms": 1000.0, "status": "completed" },
        "drain": { "elapsed_ms": 1.0, "status": "completed" },
        "cleanup": { "elapsed_ms": 1.0, "status": "completed" }
      },
      "throughput": {
        "completed_operations": 100,
        "ops_per_sec": 1000.0,
        "per_second_windows": [1000.0]
      },
      "latency": { "sample_count": 100, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 4.0 },
      "validation": {
        "errors": 0,
        "read_hits": 100,
        "read_misses": 0,
        "updates": 0,
        "scan_count_errors": 0,
        "observed_mix": "read=1.000000",
        "expected_mix_prefix": "read=100"
      },
      "drain": { "elapsed_ms": 1.0, "timed_out": false }
    }
  ]
}"#;

	#[test]
	fn parses_crud_bench_csv_aliases() {
		let rows = parse_crud_bench_csv(CSV.as_bytes()).expect("parse CSV");

		assert_eq!(required_row(&rows, "put_c", "test").unwrap().ops, 1000.0);
		assert_eq!(required_row(&rows, "put_c", "test").unwrap().p95_ms, 1.8);
		assert_eq!(required_row(&rows, "batch_create_1000", "test").unwrap().ops, 500.0);
	}

	#[test]
	fn parses_quoted_csv_fields() {
		let csv = "\
Test,Total time,Mean,Max,99th,95th,75th,50th,25th,1st,Min,IQR,OPS,CPU_avg,CPU_min,CPU_max,Memory_peak,Memory_avg,Reads,Writes,System load,System load (1m/5m/15m)
\"[B]atch::batch_create_1000 (100 batches, of \"\"1000\"\")\",1s,1.00 ms,2.00 ms,1.90 ms,1.80 ms,1.50 ms,1.00 ms,0.50 ms,0.10 ms,0.01 ms,1.00 ms,500.00,0,0,0,0,0,0,0,0,0/0/0
";

		let rows = parse_crud_bench_csv(csv.as_bytes()).expect("parse CSV");

		assert_eq!(required_row(&rows, "batch_create_1000", "test").unwrap().ops, 500.0);
	}

	#[test]
	fn parses_placeholder_latency_as_zero() {
		let csv = "\
Test,Total time,Mean,Max,99th,95th,75th,50th,25th,1st,Min,IQR,OPS,CPU_avg,CPU_min,CPU_max,Memory_peak,Memory_avg,Reads,Writes,System load,System load (1m/5m/15m)
[C]reate,1s,1.00 ms,2.00 ms,-,-,1.50 ms,1.00 ms,0.50 ms,0.10 ms,0.01 ms,1.00 ms,1000.00,0,0,0,0,0,0,0,0,0/0/0
";

		let rows = parse_crud_bench_csv(csv.as_bytes()).expect("parse CSV");

		assert_eq!(required_row(&rows, "put_c", "test").unwrap().p95_ms, 0.0);
		assert_eq!(required_row(&rows, "put_c", "test").unwrap().p99_ms, 0.0);
	}

	#[test]
	fn parses_duration_with_variable_spacing() {
		assert_eq!(parse_duration_ms("1.25ms").unwrap(), 1.25);
		assert_eq!(parse_duration_ms("1.25 ms").unwrap(), 1.25);
		assert_eq!(parse_duration_ms("1.25   ms").unwrap(), 1.25);
	}

	#[test]
	fn rejects_invalid_numeric_values() {
		for ops in ["NaN", "-1.0"] {
			let csv = format!(
				"\
Test,Total time,Mean,Max,99th,95th,75th,50th,25th,1st,Min,IQR,OPS,CPU_avg,CPU_min,CPU_max,Memory_peak,Memory_avg,Reads,Writes,System load,System load (1m/5m/15m)
[C]reate,1s,1.00 ms,2.00 ms,1.90 ms,1.80 ms,1.50 ms,1.00 ms,0.50 ms,0.10 ms,0.01 ms,1.00 ms,{ops},0,0,0,0,0,0,0,0,0/0/0
"
			);

			let err =
				parse_crud_bench_csv(csv.as_bytes()).expect_err("invalid numeric value fails");

			assert!(err.to_string().contains("must be a non-negative number"));
		}
	}

	#[test]
	fn skips_rows_with_placeholder_ops() {
		let csv = "\
Test,Total time,Mean,Max,99th,95th,75th,50th,25th,1st,Min,IQR,OPS,CPU_avg,CPU_min,CPU_max,Memory_peak,Memory_avg,Reads,Writes,System load,System load (1m/5m/15m)
[C]reate,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-
";

		let rows = parse_crud_bench_csv(csv.as_bytes()).expect("parse CSV");

		assert!(required_row(&rows, "put_c", "test").is_err());
	}

	#[test]
	fn rejects_duplicate_rows() {
		let csv = "\
Test,Total time,Mean,Max,99th,95th,75th,50th,25th,1st,Min,IQR,OPS,CPU_avg,CPU_min,CPU_max,Memory_peak,Memory_avg,Reads,Writes,System load,System load (1m/5m/15m)
[C]reate,1s,1.00 ms,2.00 ms,1.90 ms,1.80 ms,1.50 ms,1.00 ms,0.50 ms,0.10 ms,0.01 ms,1.00 ms,1000.00,0,0,0,0,0,0,0,0,0/0/0
[C]reate,1s,1.00 ms,2.00 ms,1.90 ms,1.80 ms,1.50 ms,1.00 ms,0.50 ms,0.10 ms,0.01 ms,1.00 ms,900.00,0,0,0,0,0,0,0,0,0/0/0
";

		let err = parse_crud_bench_csv(csv.as_bytes()).expect_err("duplicate row fails");

		assert!(err.to_string().contains("duplicate row"));
	}

	#[test]
	fn detects_ambiguous_row_aliases() {
		let csv = "\
Test,Total time,Mean,Max,99th,95th,75th,50th,25th,1st,Min,IQR,OPS,CPU_avg,CPU_min,CPU_max,Memory_peak,Memory_avg,Reads,Writes,System load,System load (1m/5m/15m)
[B]atch::batch_create_1000 (100 batches of 1000),1s,1.00 ms,2.00 ms,1.90 ms,1.80 ms,1.50 ms,1.00 ms,0.50 ms,0.10 ms,0.01 ms,1.00 ms,500.00,0,0,0,0,0,0,0,0,0/0/0
[B]atch::batch_create_1000 (500 batches of 1000),1s,1.00 ms,2.00 ms,1.90 ms,1.80 ms,1.50 ms,1.00 ms,0.50 ms,0.10 ms,0.01 ms,1.00 ms,600.00,0,0,0,0,0,0,0,0,0/0/0
";

		let rows = parse_crud_bench_csv(csv.as_bytes()).expect("parse CSV");
		let err =
			required_row(&rows, "batch_create_1000", "test").expect_err("ambiguous lookup fails");

		assert!(err.to_string().contains("ambiguous row"));
	}

	#[test]
	fn requires_latency_csvs_as_a_pair() {
		let err = Args::try_parse_from([
			"perf-gate",
			"--baseline-sync",
			"baseline-sync.csv",
			"--current-sync",
			"current-sync.csv",
			"--baseline-nosync",
			"baseline-nosync.csv",
			"--current-nosync",
			"current-nosync.csv",
			"--baseline-latency-sync",
			"baseline-latency-sync.csv",
		])
		.expect_err("missing current latency CSV fails");

		assert!(err.to_string().contains("--current-latency-sync"));
	}

	#[test]
	fn rejects_steady_state_rows_in_legacy_gate() {
		for row in ["balanced_zipfian", "read_heavy_zipfian", "update_heavy_zipfian"] {
			let cfg = GateConfig {
				rows: vec![row.into()],
				ratio_rows: Vec::new(),
				max_sync_regression_pct: 5.0,
				min_ratio_improvements: 0,
				max_latency_regression_pct: 5.0,
			};

			let err =
				validate_config(&cfg).expect_err("steady-state rows need a steady-state gate");

			assert!(err.to_string().contains("steady-state row"));
		}
	}

	#[test]
	fn default_steady_state_gate_includes_phase5_rows() {
		let rows = steady_state_rows_to_evaluate(Vec::new(), &[]);

		assert!(rows.contains(&"read_heavy_zipfian".to_string()));
		assert!(rows.contains(&"update_heavy_zipfian".to_string()));
	}

	#[test]
	fn optional_steady_state_rows_are_evaluated() {
		let rows = steady_state_rows_to_evaluate(
			vec!["balanced_zipfian".to_string()],
			&["read_heavy_zipfian".to_string()],
		);

		assert_eq!(rows, vec!["balanced_zipfian", "read_heavy_zipfian"]);
	}

	#[test]
	fn parses_steady_state_json_rows() {
		let rows = parse_steady_state_json(STEADY_STATE_JSON.as_bytes()).expect("parse JSON");
		let row = required_steady_state_row(&rows, "balanced_zipfian", "test").unwrap();

		assert_eq!(row.status, "completed");
		assert_eq!(row.throughput.as_ref().unwrap().completed_operations, 100);
		assert_eq!(row.latency.as_ref().unwrap().sample_count, 100);
	}

	#[test]
	fn passes_valid_steady_state_gate() {
		let cfg = steady_state_cfg(&["balanced_zipfian"]);
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0)]),
			current: steady_state_rows(&[steady_state_row("balanced_zipfian", 980.0, 2.05, 4.1)]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(eval.passed);
		assert!(eval.report.contains("toykv OPS 1000.00 -> toykv OPS 980.00"));
		assert!(eval.report.contains("Result: PASS"));
	}

	#[test]
	fn fails_invalid_steady_state_gate() {
		let cfg = steady_state_cfg(&["balanced_zipfian"]);
		let mut current = steady_state_row("balanced_zipfian", 900.0, 2.3, 4.5);
		current.validation.as_mut().unwrap().errors = 1;
		current.latency.as_mut().unwrap().sample_count = 0;
		current.drain.as_mut().unwrap().timed_out = true;
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0)]),
			current: steady_state_rows(&[current]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("validation errors"));
		assert!(eval.report.contains("missing latency samples"));
		assert!(eval.report.contains("drain timed out"));
		assert!(eval.report.contains("OPS regressed -10.00%"));
	}

	#[test]
	fn optional_unsupported_steady_state_row_is_skipped() {
		let mut cfg = steady_state_cfg(&["range_scan_uniform"]);
		cfg.optional_rows = vec!["range_scan_uniform".into()];
		let row = unsupported_steady_state_row("range_scan_uniform", Some("NotSupported"));
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(std::slice::from_ref(&row)),
			current: steady_state_rows(&[row]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(eval.passed);
	}

	#[test]
	fn optional_unsupported_steady_state_row_requires_reason() {
		let mut cfg = steady_state_cfg(&["range_scan_uniform"]);
		cfg.optional_rows = vec!["range_scan_uniform".into()];
		let row = unsupported_steady_state_row("range_scan_uniform", None);
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(std::slice::from_ref(&row)),
			current: steady_state_rows(&[row]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("unsupported without a reason"));
	}

	#[test]
	fn optional_unsupported_steady_state_row_must_match_both_artifacts() {
		let mut cfg = steady_state_cfg(&["range_scan_uniform"]);
		cfg.optional_rows = vec!["range_scan_uniform".into()];
		let unsupported = unsupported_steady_state_row("range_scan_uniform", Some("NotSupported"));
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[steady_state_row(
				"range_scan_uniform",
				1000.0,
				2.0,
				4.0,
			)]),
			current: steady_state_rows(&[unsupported]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("optional unsupported state differs"));
	}

	#[test]
	fn rejects_negative_steady_state_latency() {
		let cfg = steady_state_cfg(&["balanced_zipfian"]);
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0)]),
			current: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, -1.0, 4.0)]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("invalid latency"));
	}

	#[test]
	fn rejects_unordered_steady_state_latency() {
		let cfg = steady_state_cfg(&["balanced_zipfian"]);
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0)]),
			current: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, 10.0, 4.0)]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("unordered latency quantiles"));
	}

	#[test]
	fn rejects_invalid_steady_state_throughput_windows() {
		let cfg = steady_state_cfg(&["balanced_zipfian"]);
		let mut current = steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0);
		current.throughput.as_mut().unwrap().per_second_windows = vec![-1.0];
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0)]),
			current: steady_state_rows(&[current]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("invalid throughput windows"));
	}

	#[test]
	fn rejects_mismatched_steady_state_metadata() {
		let cfg = steady_state_cfg(&["balanced_zipfian"]);
		let baseline = steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0);
		let mut current = steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0);
		current.database = serde_json::Value::String("rocksdb".into());
		current.sync = false;
		current.task.measurement_secs = 2;
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[baseline]),
			current: steady_state_rows(&[current]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("database differs"));
		assert!(eval.report.contains("sync setting differs"));
		assert!(eval.report.contains("task metadata differs"));
	}

	#[test]
	fn rejects_idle_rows_with_operations_or_metrics() {
		let cfg = steady_state_cfg(&["idle"]);
		let mut baseline = steady_state_row("idle", 0.0, 0.0, 0.0);
		baseline.throughput = None;
		baseline.latency = None;
		baseline.validation.as_mut().unwrap().observed_mix = "none".into();
		baseline.validation.as_mut().unwrap().expected_mix_prefix = "none".into();
		let mut current = baseline.clone();
		current.validation.as_mut().unwrap().read_hits = 1;
		current.throughput = Some(SteadyStateThroughput {
			completed_operations: 1,
			ops_per_sec: 1.0,
			per_second_windows: vec![1.0],
		});
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[baseline]),
			current: steady_state_rows(&[current]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("reports client operations"));
		assert!(eval.report.contains("must not report throughput or latency"));
	}

	#[test]
	fn allows_database_mismatch_for_cross_backend_comparison() {
		let mut cfg = steady_state_cfg(&["balanced_zipfian"]);
		cfg.allow_database_mismatch = true;
		let baseline = steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0);
		let mut current = baseline.clone();
		current.database = serde_json::Value::String("rocksdb".into());
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[baseline]),
			current: steady_state_rows(&[current]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(eval.passed);
		assert!(eval.report.contains("toykv OPS 1000.00 -> rocksdb OPS 1000.00"));
		assert!(eval.report.contains("Result: PASS"));
	}

	#[test]
	fn rejects_empty_steady_state_database() {
		let cfg = steady_state_cfg(&["balanced_zipfian"]);
		let mut current = steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0);
		current.database = serde_json::Value::String(String::new());
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0)]),
			current: steady_state_rows(&[current]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("invalid database field"));
	}

	#[test]
	fn rejects_whitespace_steady_state_database() {
		let cfg = steady_state_cfg(&["balanced_zipfian"]);
		let mut current = steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0);
		current.database = serde_json::Value::String("   ".into());
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0)]),
			current: steady_state_rows(&[current]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("invalid database field"));
	}

	#[test]
	fn optional_unsupported_rows_must_be_comparable() {
		let mut cfg = steady_state_cfg(&["range_scan_uniform"]);
		cfg.optional_rows = vec!["range_scan_uniform".into()];
		let baseline = unsupported_steady_state_row("range_scan_uniform", Some("NotSupported"));
		let mut current = unsupported_steady_state_row("range_scan_uniform", Some("NotSupported"));
		current.database = serde_json::Value::String("rocksdb".into());
		current.task.records = 999;
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[baseline]),
			current: steady_state_rows(&[current]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("database differs"));
		assert!(eval.report.contains("task metadata differs"));
	}

	#[test]
	fn rejects_completed_row_with_failed_phase() {
		let cfg = steady_state_cfg(&["balanced_zipfian"]);
		let mut current = steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0);
		current.phases.cleanup.status = "failed".into();
		let inputs = SteadyStateGateInputs {
			baseline: steady_state_rows(&[steady_state_row("balanced_zipfian", 1000.0, 2.0, 4.0)]),
			current: steady_state_rows(&[current]),
		};

		let eval = evaluate_steady_state(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("completed row has non-completed cleanup phase"));
	}

	#[test]
	fn rejects_partial_steady_state_json_schema() {
		let json = r#"{
  "steady_state": [
    {
      "name": "balanced_zipfian",
      "status": "completed",
      "task": { "latency_sample_every": 1 },
      "throughput": {
        "completed_operations": 100,
        "ops_per_sec": 1000.0,
        "per_second_windows": [1000.0]
      },
      "latency": { "sample_count": 100, "p95_ms": 2.0, "p99_ms": 4.0 },
      "validation": { "errors": 0 },
      "drain": { "timed_out": false }
    }
  ]
}"#;

		let err = parse_steady_state_json(json.as_bytes()).expect_err("partial schema fails");

		assert!(err.to_string().contains("missing field"));
	}

	#[test]
	fn steady_state_mode_accepts_partial_json_parse() {
		let args =
			Args::try_parse_from(["perf-gate", "--baseline-steady-state-json", "baseline.json"])
				.expect("clap accepts partial steady-state mode");
		assert!(args.baseline_steady_state_json.is_some());
		assert!(args.current_steady_state_json.is_none());
	}

	#[test]
	fn steady_state_row_flag_selects_steady_state_mode() {
		let args = Args::try_parse_from(["perf-gate", "--steady-state-row", "balanced_zipfian"])
			.expect("steady-state row flag parses");

		assert!(uses_steady_state_mode(&args));
	}

	#[test]
	fn passes_when_ops_and_ratio_gates_hold() {
		let baseline_sync = parse_crud_bench_csv(CSV.as_bytes()).expect("parse baseline sync");
		let current_sync = rows_with_ops(&[("put_c", 1100.0), ("batch_create_1000", 550.0)]);
		let baseline_nosync = rows_with_ops(&[("put_c", 2000.0), ("batch_create_1000", 1000.0)]);
		let current_nosync = rows_with_ops(&[("put_c", 1900.0), ("batch_create_1000", 950.0)]);
		let cfg = GateConfig {
			rows: vec!["put_c".into(), "batch_create_1000".into()],
			ratio_rows: vec!["put_c".into(), "batch_create_1000".into()],
			max_sync_regression_pct: 5.0,
			min_ratio_improvements: 2,
			max_latency_regression_pct: 5.0,
		};
		let inputs = GateInputs {
			baseline_sync,
			current_sync,
			baseline_nosync,
			current_nosync,
			fjall_sync: None,
			baseline_latency_sync: None,
			current_latency_sync: None,
		};

		let eval = evaluate(&cfg, &inputs).expect("gate evaluates");
		assert!(eval.passed);
		assert!(eval.report.contains("Result: PASS"));
	}

	#[test]
	fn fails_when_sync_regresses_too_much() {
		let cfg = GateConfig {
			rows: vec!["put_c".into()],
			ratio_rows: vec!["put_c".into()],
			max_sync_regression_pct: 5.0,
			min_ratio_improvements: 0,
			max_latency_regression_pct: 5.0,
		};
		let inputs = GateInputs {
			baseline_sync: rows_with_ops(&[("put_c", 1000.0)]),
			current_sync: rows_with_ops(&[("put_c", 900.0)]),
			baseline_nosync: rows_with_ops(&[("put_c", 2000.0)]),
			current_nosync: rows_with_ops(&[("put_c", 2000.0)]),
			fjall_sync: None,
			baseline_latency_sync: None,
			current_latency_sync: None,
		};

		let eval = evaluate(&cfg, &inputs).expect("gate evaluates");
		assert!(!eval.passed);
		assert!(eval.report.contains("regressed -10.00%"));
	}

	#[test]
	fn rejects_impossible_min_ratio_improvements() {
		let cfg = GateConfig {
			rows: vec!["put_c".into()],
			ratio_rows: vec!["put_c".into()],
			max_sync_regression_pct: 5.0,
			min_ratio_improvements: 2,
			max_latency_regression_pct: 5.0,
		};

		let err = validate_config(&cfg).expect_err("config fails");

		assert!(err.to_string().contains("cannot be greater than the number of ratio rows"));
	}

	#[test]
	fn rejects_negative_regression_thresholds() {
		let mut cfg = GateConfig {
			rows: vec!["put_c".into()],
			ratio_rows: vec!["put_c".into()],
			max_sync_regression_pct: -1.0,
			min_ratio_improvements: 0,
			max_latency_regression_pct: 5.0,
		};

		let err = validate_config(&cfg).expect_err("negative sync threshold fails");
		assert!(err.to_string().contains("--max-sync-regression-pct cannot be negative"));

		cfg.max_sync_regression_pct = 5.0;
		cfg.max_latency_regression_pct = -1.0;

		let err = validate_config(&cfg).expect_err("negative latency threshold fails");
		assert!(err.to_string().contains("--max-latency-regression-pct cannot be negative"));
	}

	#[test]
	fn allows_fjall_relative_difference_within_tolerance() {
		let cfg = GateConfig {
			rows: vec!["put_c".into()],
			ratio_rows: vec!["put_c".into()],
			max_sync_regression_pct: 5.0,
			min_ratio_improvements: 0,
			max_latency_regression_pct: 5.0,
		};
		let inputs = GateInputs {
			baseline_sync: rows_with_ops(&[("put_c", 100.0)]),
			current_sync: rows_with_ops(&[("put_c", 96.0)]),
			baseline_nosync: rows_with_ops(&[("put_c", 2000.0)]),
			current_nosync: rows_with_ops(&[("put_c", 2000.0)]),
			fjall_sync: Some(rows_with_ops(&[("put_c", 100.0)])),
			baseline_latency_sync: None,
			current_latency_sync: None,
		};

		let eval = evaluate(&cfg, &inputs).expect("gate evaluates");

		assert!(eval.passed);
		assert!(eval.report.contains("Result: PASS"));
	}

	#[test]
	fn fails_when_fjall_relative_difference_exceeds_tolerance() {
		let cfg = GateConfig {
			rows: vec!["put_c".into()],
			ratio_rows: vec!["put_c".into()],
			max_sync_regression_pct: 5.0,
			min_ratio_improvements: 0,
			max_latency_regression_pct: 5.0,
		};
		let inputs = GateInputs {
			baseline_sync: rows_with_ops(&[("put_c", 1000.0)]),
			current_sync: rows_with_ops(&[("put_c", 94.0)]),
			baseline_nosync: rows_with_ops(&[("put_c", 2000.0)]),
			current_nosync: rows_with_ops(&[("put_c", 2000.0)]),
			fjall_sync: Some(rows_with_ops(&[("put_c", 100.0)])),
			baseline_latency_sync: None,
			current_latency_sync: None,
		};

		let eval = evaluate(&cfg, &inputs).expect("gate evaluates");

		assert!(!eval.passed);
		assert!(eval.report.contains("below -5.00%"));
	}

	#[test]
	fn fails_when_latency_regresses_too_much() {
		let cfg = GateConfig {
			rows: vec!["put_c".into()],
			ratio_rows: vec!["put_c".into()],
			max_sync_regression_pct: 5.0,
			min_ratio_improvements: 0,
			max_latency_regression_pct: 5.0,
		};
		let inputs = GateInputs {
			baseline_sync: rows_with_ops(&[("put_c", 1000.0)]),
			current_sync: rows_with_ops(&[("put_c", 1000.0)]),
			baseline_nosync: rows_with_ops(&[("put_c", 2000.0)]),
			current_nosync: rows_with_ops(&[("put_c", 1900.0)]),
			fjall_sync: None,
			baseline_latency_sync: Some(rows_with_latency(&[("put_c", 1.0, 2.0)])),
			current_latency_sync: Some(rows_with_latency(&[("put_c", 1.2, 2.3)])),
		};

		let eval = evaluate(&cfg, &inputs).expect("gate evaluates");
		assert!(!eval.passed);
		assert!(eval.report.contains("p95 regressed 20.00%"));
	}

	#[test]
	fn fails_when_latency_baseline_placeholder_becomes_measured() {
		let cfg = GateConfig {
			rows: vec!["put_c".into()],
			ratio_rows: vec!["put_c".into()],
			max_sync_regression_pct: 5.0,
			min_ratio_improvements: 0,
			max_latency_regression_pct: 5.0,
		};
		let inputs = GateInputs {
			baseline_sync: rows_with_ops(&[("put_c", 1000.0)]),
			current_sync: rows_with_ops(&[("put_c", 1000.0)]),
			baseline_nosync: rows_with_ops(&[("put_c", 2000.0)]),
			current_nosync: rows_with_ops(&[("put_c", 1900.0)]),
			fjall_sync: None,
			baseline_latency_sync: Some(rows_with_latency(&[("put_c", 0.0, 0.0)])),
			current_latency_sync: Some(rows_with_latency(&[("put_c", 1.0, 2.0)])),
		};

		let eval = evaluate(&cfg, &inputs).expect("gate evaluates");
		assert!(!eval.passed);
		assert!(eval.report.contains("p95 regressed inf%"));
	}

	fn rows_with_ops(rows: &[(&str, f64)]) -> BenchCsv {
		rows.iter()
			.map(|(name, ops)| {
				(
					(*name).to_string(),
					BenchRow {
						ops: *ops,
						p95_ms: 1.0,
						p99_ms: 2.0,
					},
				)
			})
			.collect()
	}

	fn rows_with_latency(rows: &[(&str, f64, f64)]) -> BenchCsv {
		rows.iter()
			.map(|(name, p95_ms, p99_ms)| {
				(
					(*name).to_string(),
					BenchRow {
						ops: 1.0,
						p95_ms: *p95_ms,
						p99_ms: *p99_ms,
					},
				)
			})
			.collect()
	}

	fn steady_state_cfg(rows: &[&str]) -> SteadyStateGateConfig {
		SteadyStateGateConfig {
			rows: rows.iter().map(|row| (*row).to_string()).collect(),
			optional_rows: Vec::new(),
			max_ops_regression_pct: 5.0,
			max_latency_regression_pct: 5.0,
			allow_database_mismatch: false,
		}
	}

	fn steady_state_rows(rows: &[SteadyStateRow]) -> SteadyStateRows {
		rows.iter().cloned().map(|row| (row.name.clone(), row)).collect()
	}

	fn steady_state_row(name: &str, ops: f64, p95_ms: f64, p99_ms: f64) -> SteadyStateRow {
		SteadyStateRow {
			name: name.into(),
			suite: "steady-state".into(),
			database: serde_json::Value::String("toykv".into()),
			status: "completed".into(),
			unsupported_reason: None,
			failure_reason: None,
			sync: true,
			task: SteadyStateTask {
				records: 100,
				clients: 1,
				threads: 1,
				warmup_secs: 0,
				measurement_secs: 1,
				latency_sample_every: 1,
				seed: 1,
				worker_seed_derivation: "splitmix64(seed ^ worker_index)".into(),
				operation_mix: "read=1.000000".into(),
				operation_mix_period: 1000,
				key_selection: "scrambled_zipfian".into(),
				zipfian_exponent: 0.99,
			},
			phases: completed_steady_state_phases(),
			throughput: Some(SteadyStateThroughput {
				completed_operations: 100,
				ops_per_sec: ops,
				per_second_windows: vec![ops],
			}),
			latency: Some(SteadyStateLatency {
				sample_count: 100,
				p50_ms: p95_ms / 2.0,
				p95_ms,
				p99_ms,
			}),
			validation: Some(SteadyStateValidation {
				errors: 0,
				read_hits: 100,
				read_misses: 0,
				updates: 0,
				scan_count_errors: 0,
				observed_mix: "read=1.000000".into(),
				expected_mix_prefix: "read=100".into(),
			}),
			drain: Some(SteadyStateDrain {
				elapsed_ms: 1.0,
				timed_out: false,
			}),
		}
	}

	fn unsupported_steady_state_row(name: &str, reason: Option<&str>) -> SteadyStateRow {
		SteadyStateRow {
			name: name.into(),
			suite: "steady-state".into(),
			database: serde_json::Value::String("toykv".into()),
			status: "unsupported".into(),
			unsupported_reason: reason.map(str::to_string),
			failure_reason: None,
			sync: true,
			task: SteadyStateTask {
				records: 100,
				clients: 1,
				threads: 1,
				warmup_secs: 0,
				measurement_secs: 1,
				latency_sample_every: 1,
				seed: 1,
				worker_seed_derivation: "splitmix64(seed ^ worker_index)".into(),
				operation_mix: "scan=1.000000".into(),
				operation_mix_period: 1000,
				key_selection: "uniform".into(),
				zipfian_exponent: 0.99,
			},
			phases: completed_steady_state_phases(),
			throughput: None,
			latency: None,
			validation: None,
			drain: None,
		}
	}

	fn completed_steady_state_phases() -> SteadyStatePhases {
		let phase = SteadyStatePhase {
			elapsed_ms: 1.0,
			status: "completed".into(),
		};
		SteadyStatePhases {
			prepare: phase.clone(),
			warmup: phase.clone(),
			measure: phase.clone(),
			drain: phase.clone(),
			cleanup: phase,
		}
	}
}
