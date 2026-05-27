use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tickbar::{BarAggregator, Tick, TimeAlignment};

fn generate_ticks(count: usize) -> Vec<Tick> {
    let mut ticks = Vec::with_capacity(count);
    for i in 0..count {
        ticks.push(Tick {
            timestamp_nanos: i as i64 * 1_000_000_000,
            price: 100_000_000 + (i % 1000) as i64,
            volume: 1_000_000,
            flags: 0,
        });
    }
    ticks
}

fn bench_1m_ticks_to_1min_bars(c: &mut Criterion) {
    let ticks = generate_ticks(1_000_000);

    c.bench_function("aggregate_1m_ticks", |b| {
        b.iter(|| {
            let mut agg = BarAggregator::new(
                60_000_000_000,
                TimeAlignment::UTC,
                8,
                0,
                false,
                false,
                ticks.first().map_or(0, |t| t.timestamp_nanos),
            );
            let _ = agg.ingest_ticks(black_box(&ticks));
            agg.finalize()
        });
    });
}

criterion_group!(benches, bench_1m_ticks_to_1min_bars);
criterion_main!(benches);
