use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;

const DEFAULT_ROWS: &[&str] =
	&["put_c", "batch_create_100", "batch_create_1000", "batch_delete_100", "batch_delete_1000"];
const DEFAULT_RATIO_ROWS: &[&str] = &["put_c", "batch_create_1000", "batch_delete_1000"];

#[derive(Parser, Debug)]
#[command(name = "perf-gate")]
#[command(about = "Check crud-bench sync performance gates for ToyKV artifacts")]
struct Args {
	/// Previous ToyKV --sync crud-bench CSV.
	#[arg(long)]
	baseline_sync: PathBuf,
	/// Current ToyKV --sync crud-bench CSV.
	#[arg(long)]
	current_sync: PathBuf,
	/// Previous ToyKV no-sync crud-bench CSV.
	#[arg(long)]
	baseline_nosync: PathBuf,
	/// Current ToyKV no-sync crud-bench CSV.
	#[arg(long)]
	current_nosync: PathBuf,
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

struct Evaluation {
	report: String,
	passed: bool,
}

type BenchCsv = HashMap<String, BenchRow>;

fn main() -> Result<ExitCode> {
	let args = Args::parse();
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
		baseline_sync: read_crud_bench_csv(&args.baseline_sync)?,
		current_sync: read_crud_bench_csv(&args.current_sync)?,
		baseline_nosync: read_crud_bench_csv(&args.baseline_nosync)?,
		current_nosync: read_crud_bench_csv(&args.current_nosync)?,
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

fn read_crud_bench_csv(path: &Path) -> Result<BenchCsv> {
	let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
	parse_crud_bench_csv(file).with_context(|| format!("failed to parse {}", path.display()))
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

fn validate_config(cfg: &GateConfig) -> Result<()> {
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
}
