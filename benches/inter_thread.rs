use ipc_communication::{communication, Communication};

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use nix::{
    sys::wait::waitpid,
    unistd::{fork, ForkResult},
};
use rand::{distributions::Standard, Rng, SeedableRng};
use rand_xoshiro::Xoshiro256PlusPlus;
use std::{convert::identity, mem};

fn transformation(request: Vec<u8>) -> usize {
    request.into_iter().map(usize::from).sum()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(100_500);

    const CHANNELS: usize = 4;
    let Communication {
        client_builder,
        processors,
        handle,
    } = communication::<Vec<u8>, usize>(CHANNELS).unwrap();

    let processors =
        std::thread::spawn(move || processors.run_in_parallel(|_| transformation).unwrap());

    const MAX_MESSAGE_LEN: usize = 1024;
    const MESSAGES: usize = 100;
    let data = (0..MESSAGES)
        .map(|_| {
            let len = rng.gen_range(0, MAX_MESSAGE_LEN);
            let data = (&mut rng)
                .sample_iter(Standard)
                .take(len)
                .collect::<Vec<u8>>();
            let channel_id = rng.gen_range(0, CHANNELS);
            (channel_id, data)
        })
        .collect::<Vec<_>>();

    let data_size = data
        .iter()
        .map(|(_chan_id, v)| mem::size_of_val(_chan_id) + v.len())
        .sum::<usize>();
    println!("Total data size: {}", data_size);

    let mut group = c.benchmark_group("Throughput");
    group.throughput(Throughput::Bytes(data_size as u64));
    group.bench_function("communication", |bencher| {
        let client = client_builder.build();
        bencher.iter_batched(
            || data.clone(),
            |data| {
                let mut total = 0;
                for (channel_id, data) in data {
                    total += client.make_request(channel_id, data)?;
                }
                Ok::<_, ipc_communication::Error>(total)
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();

    for &size in &[8, 64, 512, 1024, 1024 * 1024] {
        let mut group = c.benchmark_group(format!("Latency at {}", size));
        group.bench_function("ping-threads", |bencher| {
            let Communication {
                client_builder,
                processors,
                handle,
            } = communication::<Vec<u8>, Vec<u8>>(1).unwrap();

            let processors =
                std::thread::spawn(move || processors.run_in_parallel(|_| identity).unwrap());
            let client = client_builder.build();
            bencher.iter_batched(
                || (&mut rng).sample_iter(Standard).take(size).collect(),
                |vec| client.make_request(0, vec).unwrap(),
                BatchSize::LargeInput,
            );
            handle.stop().unwrap();
            processors.join().unwrap();
        });
        group.bench_function("ping-processes", |bencher| {
            let Communication {
                client_builder,
                processors,
                handle,
            } = communication::<Vec<u8>, Vec<u8>>(1).unwrap();

            let pid = match fork().unwrap() {
                ForkResult::Parent { child } => child,
                ForkResult::Child => {
                    let _ = processors.run_in_parallel(|_| identity).unwrap();
                    std::process::exit(0);
                }
            };

            let client = client_builder.build();
            bencher.iter_batched(
                || (&mut rng).sample_iter(Standard).take(size).collect(),
                |vec| client.make_request(0, vec).unwrap(),
                BatchSize::LargeInput,
            );
            handle.stop().unwrap();
            waitpid(pid, None).unwrap();
        });
        group.finish();
    }

    handle.stop().unwrap();
    let _ = processors.join();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
