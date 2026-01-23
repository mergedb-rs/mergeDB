use criterion::{Criterion, criterion_group, criterion_main};
use mergedb_types::{Merge, pn_counter::PNCounter};

fn benchmark_counter_merge(c: &mut Criterion) {
    let mut c1 = PNCounter::new("node_1".to_string(), 0, 0);
    let mut c2 = PNCounter::new("node_2".to_string(), 0, 0);

    for _ in 0..1000 {
        c1.increment("node_1".to_string(), 1);
        c2.increment("node_2".to_string(), 1);
    }

    c.bench_function("merge_1000_counter_updates", |b| {
        b.iter_batched(
            || (c1.clone(), c2.clone()), //setup part, is not counted in benchmark time
            |(mut target, mut source)| {
                target.merge(&mut source);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, benchmark_counter_merge);
criterion_main!(benches);
