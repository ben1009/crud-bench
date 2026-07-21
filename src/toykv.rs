#![cfg(feature = "toykv")]

use crate::benchmark::NOT_SUPPORTED_ERROR;
use crate::engine::{BenchmarkClient, BenchmarkEngine, ScanContext};
use crate::value::BenchValue;
use crate::valueprovider::Columns;
use crate::{Benchmark, KeyType, Projection, Scan};
use anyhow::{Result, anyhow, bail};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use toykv::compact::{CompactionOptions, LeveledCompactionOptions};
use toykv::lsm_storage::{KvEngine, LsmStorageOptions, WriteBatchRecord};
use toykv::vlog::ValueSeparationOptions;

const DATABASE_DIR: &str = "toykv_data";

fn env_flag(name: &str) -> bool {
	std::env::var(name).is_ok()
}

pub(crate) struct ToyKvClientProvider {
	engine: Arc<KvEngine>,
	reads_only: bool,
	preserve_db: bool,
	load_only: bool,
}

impl BenchmarkEngine<ToyKvClient> for ToyKvClientProvider {
	fn wait_timeout(&self) -> Option<Duration> {
		None
	}

	async fn setup(_kt: KeyType, _columns: Columns, options: &Benchmark) -> Result<Self> {
		let reads_only = env_flag("READS_ONLY");
		let preserve_db = env_flag("PRESERVE_DB");
		let load_only = env_flag("LOAD_ONLY");

		// Cleanup the data directory (skip if READS_ONLY or PRESERVE_DB to reuse existing data)
		if !reads_only && !preserve_db {
			std::fs::remove_dir_all(DATABASE_DIR).ok();
		}

		let compaction_opts = CompactionOptions::Leveled(LeveledCompactionOptions {
			level0_file_num_compaction_trigger: 2,
			max_levels: 4,
			base_level_size_mb: 128,
			level_size_multiplier: 2,
		});

		let storage_opts = LsmStorageOptions {
			block_size: 64 * 1024,      // 64KB — match Fjall
			target_sst_size: 256 << 20, // 256MB — match Fjall
			num_memtable_limit: 50,
			compaction_options: compaction_opts,
			enable_wal: options.sync,
			serializable: false,
			value_separation: Some(ValueSeparationOptions {
				enabled: true,
				min_value_size: 4 * 1024, // 4KB — match Fjall
				..Default::default()
			}),
			manifest_snapshot_threshold_bytes: 4 * 1024 * 1024,
			block_cache_capacity: 524_288, // ~32GB with 64KB blocks, close to Fjall's ~46GB
			enable_cache_backfill: true,
			prefix_bloom: Default::default(),
		};

		let engine = KvEngine::open(DATABASE_DIR, storage_opts)?;

		Ok(Self {
			engine,
			reads_only,
			preserve_db,
			load_only,
		})
	}

	async fn create_client(&self) -> Result<ToyKvClient> {
		Ok(ToyKvClient {
			engine: self.engine.clone(),
			reads_only: self.reads_only,
			preserve_db: self.preserve_db,
			load_only: self.load_only,
		})
	}
}

pub(crate) struct ToyKvClient {
	engine: Arc<KvEngine>,
	reads_only: bool,
	preserve_db: bool,
	load_only: bool,
}

impl BenchmarkClient for ToyKvClient {
	type ReadRow = BenchValue;

	async fn shutdown(&self) -> Result<()> {
		self.engine.close()?;
		if !self.reads_only && !self.preserve_db {
			std::fs::remove_dir_all(DATABASE_DIR).ok();
		}
		Ok(())
	}

	async fn create_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		if self.reads_only {
			return Ok(());
		}
		let encoded = val.encode()?;
		self.engine.put(&key.to_ne_bytes(), &encoded)?;
		Ok(())
	}

	async fn create_string(&self, key: String, val: BenchValue) -> Result<()> {
		if self.reads_only {
			return Ok(());
		}
		let encoded = val.encode()?;
		self.engine.put(key.as_bytes(), &encoded)?;
		Ok(())
	}

	async fn read_u32(&self, key: u32) -> Result<BenchValue> {
		let res = self.engine.get(&key.to_ne_bytes())?;
		let bytes = res.ok_or_else(|| anyhow!("key should exist"))?;
		let val = BenchValue::decode(&bytes)?;
		Ok(black_box(val))
	}

	async fn read_string(&self, key: String) -> Result<BenchValue> {
		let res = self.engine.get(key.as_bytes())?;
		let bytes = res.ok_or_else(|| anyhow!("key should exist"))?;
		let val = BenchValue::decode(&bytes)?;
		Ok(black_box(val))
	}

	async fn update_u32(&self, key: u32, val: BenchValue) -> Result<()> {
		if self.reads_only {
			return Ok(());
		}
		let encoded = val.encode()?;
		self.engine.put(&key.to_ne_bytes(), &encoded)?;
		Ok(())
	}

	async fn update_string(&self, key: String, val: BenchValue) -> Result<()> {
		if self.reads_only {
			return Ok(());
		}
		let encoded = val.encode()?;
		self.engine.put(key.as_bytes(), &encoded)?;
		Ok(())
	}

	async fn delete_u32(&self, key: u32) -> Result<()> {
		if self.reads_only || self.load_only {
			return Ok(());
		}
		self.engine.delete(&key.to_ne_bytes())?;
		Ok(())
	}

	async fn delete_string(&self, key: String) -> Result<()> {
		if self.reads_only || self.load_only {
			return Ok(());
		}
		self.engine.delete(key.as_bytes())?;
		Ok(())
	}

	async fn scan_u32(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		self.do_scan(scan)
	}

	async fn scan_string(&self, scan: &Scan, _ctx: ScanContext) -> Result<usize> {
		if scan.condition.is_some() {
			bail!(NOT_SUPPORTED_ERROR);
		}
		self.do_scan(scan)
	}

	async fn batch_create_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		if self.reads_only {
			return Ok(());
		}
		let batch: Vec<WriteBatchRecord<Vec<u8>>> = key_vals
			.map(|(k, v)| Ok(WriteBatchRecord::Put(k.to_ne_bytes().to_vec(), v.encode()?)))
			.collect::<Result<_>>()?;
		self.engine.write_batch(&batch)?;
		Ok(())
	}

	async fn batch_create_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		if self.reads_only {
			return Ok(());
		}
		let batch: Vec<WriteBatchRecord<Vec<u8>>> = key_vals
			.map(|(k, v)| Ok(WriteBatchRecord::Put(k.into_bytes(), v.encode()?)))
			.collect::<Result<_>>()?;
		self.engine.write_batch(&batch)?;
		Ok(())
	}

	async fn batch_read_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		let key_bytes: Vec<[u8; 4]> = keys.map(|k| k.to_ne_bytes()).collect();
		let key_refs: Vec<&[u8]> = key_bytes.iter().map(|k| k.as_slice()).collect();
		let results = self.engine.batch_get(&key_refs);
		for res in results {
			let bytes = res?.ok_or_else(|| anyhow!("key should exist"))?;
			let val = BenchValue::decode(&bytes)?;
			black_box(val);
		}
		Ok(())
	}

	async fn batch_read_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		let key_owned: Vec<String> = keys.collect();
		let key_refs: Vec<&[u8]> = key_owned.iter().map(|k| k.as_bytes()).collect();
		let results = self.engine.batch_get(&key_refs);
		for res in results {
			let bytes = res?.ok_or_else(|| anyhow!("key should exist"))?;
			let val = BenchValue::decode(&bytes)?;
			black_box(val);
		}
		Ok(())
	}

	async fn batch_update_u32(
		&self,
		key_vals: impl Iterator<Item = (u32, BenchValue)> + Send,
	) -> Result<()> {
		if self.reads_only {
			return Ok(());
		}
		let batch: Vec<WriteBatchRecord<Vec<u8>>> = key_vals
			.map(|(k, v)| Ok(WriteBatchRecord::Put(k.to_ne_bytes().to_vec(), v.encode()?)))
			.collect::<Result<_>>()?;
		self.engine.write_batch(&batch)?;
		Ok(())
	}

	async fn batch_update_string(
		&self,
		key_vals: impl Iterator<Item = (String, BenchValue)> + Send,
	) -> Result<()> {
		if self.reads_only {
			return Ok(());
		}
		let batch: Vec<WriteBatchRecord<Vec<u8>>> = key_vals
			.map(|(k, v)| Ok(WriteBatchRecord::Put(k.into_bytes(), v.encode()?)))
			.collect::<Result<_>>()?;
		self.engine.write_batch(&batch)?;
		Ok(())
	}

	async fn batch_delete_u32(&self, keys: impl Iterator<Item = u32> + Send) -> Result<()> {
		if self.reads_only || self.load_only {
			return Ok(());
		}
		let batch: Vec<WriteBatchRecord<Vec<u8>>> =
			keys.map(|k| WriteBatchRecord::Del(k.to_ne_bytes().to_vec())).collect();
		self.engine.write_batch(&batch)?;
		Ok(())
	}

	async fn batch_delete_string(&self, keys: impl Iterator<Item = String> + Send) -> Result<()> {
		if self.reads_only || self.load_only {
			return Ok(());
		}
		let batch: Vec<WriteBatchRecord<Vec<u8>>> =
			keys.map(|k| WriteBatchRecord::Del(k.into_bytes())).collect();
		self.engine.write_batch(&batch)?;
		Ok(())
	}
}

impl ToyKvClient {
	fn do_scan(&self, scan: &Scan) -> Result<usize> {
		let s = scan.start.unwrap_or(0);
		let l = scan.limit.unwrap_or(usize::MAX);
		let p = scan.projection()?;

		// Full range scan
		let mut iter = self.engine.scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;

		if iter.skip_entries(s)? < s {
			return Ok(0);
		}

		match p {
			Projection::Id => iter.visit_keys(l, |key| {
				black_box(key);
			}),
			Projection::Full => iter.visit_values(l, |value| {
				black_box(value);
			}),
			Projection::Count => iter.count_entries(l),
		}
	}
}
