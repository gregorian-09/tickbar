# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-05-27

### Added

- Tick-to-bar aggregation engine for time, volume, tick, and dollar bars
- Builder API (`TickAggregator::new()`, builder pattern)
- Streaming API with `push_tick` and `drain_completed`
- Batch ingestion: `push_ticks`, `push_from_buffer`, `push_from_numpy`, `push_from_arrays`, `push_from_bytes`
- `ingest_ticks_unchecked` for maximum throughput (skips per-tick ordering check)
- Gap filling via `fill_value_gaps`/`fill_time_gaps`
- `resample` for converting between bar intervals
- Parallel multi-symbol aggregation (`aggregate_parallel`)
- Export formats: CSV (string), Arrow IPC, Polars DataFrame
- Memory-mapped file ingestion (`TickDataSource::Mmap`)
- Python bindings via PyO3 with `abi3-py311` support
- Python type stubs (`python/tickbar.pyi`)
- Comprehensive test suite: 25 unit tests, 9 integration tests, 8 doc tests
- Documentation: self-sufficient rustdoc with 8 runnable examples, PyPI readme with end-to-end workflows

### Performance

- `push_from_buffer` benchmark: ~6.7M ticks/s (1.03× faster than `push_from_numpy`)
- `ingest_ticks_unchecked` ~6× speedup over ordered path on real-world data
- `ingest_from_arrays` avoids intermediate `Vec<Tick>` allocation
