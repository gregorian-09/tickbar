use std::time::Duration;
use tickbar::{
    aggregate_parallel, BarAggregator, Tick, TickAggregator, TimeAlignment,
};
#[cfg(any(feature = "arrow-export", feature = "polars-export"))]
use tickbar::BarSeries;

fn make_tick(ts_nanos: i64, price: i64, volume: i64) -> Tick {
    Tick {
        timestamp_nanos: ts_nanos,
        price,
        volume,
        flags: 0,
    }
}

#[test]
fn test_full_pipeline_1min_bars() {
    let mut ticks = Vec::with_capacity(1000);
    for i in 0..1000 {
        let price = 100_000_000 + (i % 50) as i64;
        ticks.push(make_tick(i as i64 * 1_000_000_000, price, 1_000_000));
    }

    let mut agg = BarAggregator::new(
        60_000_000_000,
        TimeAlignment::UTC,
        8,
        0,
        false,
        false,
        ticks.first().unwrap().timestamp_nanos,
    );
    agg.ingest_ticks(&ticks).unwrap();
    let series = agg.finalize();

    assert!(!series.as_slice().is_empty(), "should produce bars");
    for bar in series.as_slice() {
        assert!(bar.high >= bar.low, "high must be >= low");
        assert!(bar.high >= bar.open, "high must be >= open");
        assert!(bar.high >= bar.close, "high must be >= close");
        assert!(bar.low <= bar.open, "low must be <= open");
        assert!(bar.low <= bar.close, "low must be <= close");
        assert!(bar.volume >= 0, "volume must be non-negative");
        assert!(bar.tick_count > 0, "each bar must have ticks");
    }
}

#[test]
fn test_tick_aggregator_builder_api() {
    let mut agg = TickAggregator::builder()
        .interval(Duration::from_secs(60))
        .symbol("AAPL")
        .build()
        .expect("valid config");

    for i in 0..100 {
        agg.push_tick(make_tick(i as i64 * 1_000_000_000, 100_000_000 + i as i64, 1000))
            .expect("push should succeed");
    }

    let series = agg.finalize();
    assert_eq!(series.symbol(), "AAPL");
    for bar in series.as_slice() {
        assert!(bar.tick_count > 0);
    }
}

#[test]
fn test_gap_filling_integration() {
    let ticks = vec![
        make_tick(0, 100_000_000, 1000),
        make_tick(180_000_000_000, 200_000_000, 500),
    ];

    let mut agg = BarAggregator::new(
        60_000_000_000,
        TimeAlignment::UTC,
        8,
        0,
        true,
        false,
        0,
    );
    agg.ingest_ticks(&ticks).unwrap();
    let series = agg.finalize();

    // 0-60s: tick bar, 60-120s: empty, 120-180s: empty, 180-240s: tick bar
    assert_eq!(series.as_slice().len(), 4);
    assert_eq!(series.as_slice()[0].close, 100_000_000);
    assert_eq!(series.as_slice()[1].close, 100_000_000);
    assert_eq!(series.as_slice()[2].close, 100_000_000);
    assert_eq!(series.as_slice()[3].close, 200_000_000);
}

#[test]
fn test_parallel_multi_symbol() {
    use std::collections::HashMap;

    let mut by_symbol: HashMap<String, Vec<Tick>> = HashMap::new();
    for sym in &["AAPL", "GOOG", "MSFT"] {
        let ticks: Vec<Tick> = (0..500)
            .map(|i| make_tick(i * 60_000_000_000, 100_000_000 + i, 1000))
            .collect();
        by_symbol.insert(sym.to_string(), ticks);
    }

    let config = tickbar::AggregatorConfig {
        interval_nanos: 300_000_000_000,
        alignment: TimeAlignment::UTC,
        fill_gaps: false,
        forward_fill: false,
        price_decimals: 8,
        volume_decimals: 0,
    };

    let results = aggregate_parallel(by_symbol, config);
    assert_eq!(results.len(), 3);
    for (sym, res) in &results {
        let series = res.as_ref().expect("aggregation should succeed");
        assert!(!series.as_slice().is_empty(), "{sym} should have bars");
    }
}

#[test]
fn test_resample_bars() {
    let mut ticks = Vec::with_capacity(5000);
    for i in 0..5000 {
        ticks.push(make_tick(i as i64 * 1_000_000_000, 100_000_000, 1000));
    }

    let mut agg = BarAggregator::new(
        60_000_000_000,
        TimeAlignment::UTC,
        8,
        0,
        false,
        false,
        0,
    );
    agg.ingest_ticks(&ticks).unwrap();
    let series = agg.finalize();

    let five_min = series.resample(300_000_000_000).expect("resample should work");
    assert!(five_min.as_slice().len() < series.as_slice().len());
    assert_eq!(
        five_min.as_slice().len() * 5,
        series.as_slice().len() + 1 // last partial bar is included
    );
}

#[test]
fn test_out_of_order_rejected() {
    let mut agg = TickAggregator::builder()
        .interval(Duration::from_secs(60))
        .build()
        .expect("valid config");

    agg.push_tick(make_tick(100, 100_000_000, 1000))
        .expect("first tick ok");
    let err = agg.push_tick(make_tick(50, 101_000_000, 500));
    assert!(err.is_err(), "out-of-order must be rejected");
}

#[test]
fn test_csv_round_trip() {
    let mut agg = BarAggregator::new(
        60_000_000_000,
        TimeAlignment::UTC,
        8,
        0,
        false,
        false,
        0,
    );
    let ticks: Vec<_> = (0..10)
        .map(|i| make_tick(i * 1_000_000_000, 100_000_000 + i * 1000, 1000))
        .collect();
    agg.ingest_ticks(&ticks).unwrap();
    let series = agg.finalize();

    let mut buf = Vec::new();
    let mut writer = csv::Writer::from_writer(&mut buf);
    series.to_csv(&mut writer).expect("csv export ok");
    drop(writer);

    let output = String::from_utf8(buf).expect("valid utf8");
    assert!(output.contains("100000000"), "should contain price data: {output}");
    assert!(output.lines().count() > 0);
}

#[test]
#[cfg(feature = "arrow-export")]
fn test_arrow_export() {
    let mut series = BarSeries::new("TEST", 60_000_000_000);
    series.push(tickbar::Bar {
        timestamp_nanos: 0,
        open: 100,
        high: 110,
        low: 90,
        close: 105,
        volume: 1000,
        tick_count: 5,
        vwap: 102,
    });
    let batch = series.to_arrow().expect("arrow export ok");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 8);
}

#[test]
#[cfg(feature = "polars-export")]
fn test_polars_export() {
    let mut series = BarSeries::new("TEST", 60_000_000_000);
    series.push(tickbar::Bar {
        timestamp_nanos: 0,
        open: 100,
        high: 110,
        low: 90,
        close: 105,
        volume: 1000,
        tick_count: 5,
        vwap: 102,
    });
    let df = series.to_polars().expect("polars export ok");
    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 8);
}
