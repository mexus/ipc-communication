use ipc_communication::{communication, Communication};
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Normal, Standard};
use rand_xoshiro::Xoshiro256PlusPlus;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    thread::{sleep, spawn, JoinHandle},
    time::{Duration, Instant},
};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Config {
    /// Amount of channels.
    #[structopt(long)]
    channels: usize,

    /// Amount of clients.
    #[structopt(long)]
    clients: usize,

    /// Mean message size, in bytes.
    #[structopt(long = "mean", default_value = "1024")]
    message_size_mean: usize,

    /// Standard deviation of a message size.
    #[structopt(long = "stddev", default_value = "10")]
    message_size_stddev: usize,

    /// Random seed.
    #[structopt(long, default_value = "519550570373")]
    seed: u64,

    /// Timeout.
    #[structopt(long, default_value = "1 min", parse(try_from_str = humantime::parse_duration))]
    timeout: Duration,

    /// Minimal delay between requests.
    #[structopt(long, default_value = "1 ns", parse(try_from_str = humantime::parse_duration))]
    min_delay: Duration,
}

fn run_client<R, D>(
    ipc_client: ipc_communication::Client<Vec<u8>, Vec<u8>>,
    mut rng: R,
    msg_size_distr: D,
    run_until: Instant,
    min_delay: Duration,
) -> JoinHandle<Result<usize, ipc_communication::Error>>
where
    D: Distribution<f64> + Send + 'static,
    R: Rng + Send + 'static,
{
    const MIN_SLEEP: Duration = Duration::from_nanos(10);
    const MAX_MSG_LEN: usize = 10 * 1024 * 1024;

    spawn(move || {
        let mut previous = Instant::now()
            .checked_sub(2 * min_delay)
            .unwrap_or_else(Instant::now);
        let mut sent = 0;
        loop {
            let now = Instant::now();
            if now >= run_until {
                break Ok(sent);
            }

            let msg_len = msg_size_distr.sample(&mut rng).abs().round();
            let msg_len = if msg_len > MAX_MSG_LEN as f64 {
                MAX_MSG_LEN
            } else {
                msg_len as usize
            };

            sent += msg_len;
            let msg: Vec<u8> = (&mut rng).sample_iter(Standard).take(msg_len).collect();
            let channel_id = rng.gen_range(0, ipc_client.channels());

            let to_sleep = now - previous;
            if to_sleep > MIN_SLEEP {
                sleep(to_sleep);
            }
            previous = Instant::now();

            let response = ipc_client.make_request(channel_id, msg)?;
            assert_eq!(response.len(), msg_len);
        }
    })
}

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("std_dev < 0 or nan: {}", stddev))]
    NormalDistr { stddev: usize },

    #[snafu(display("IPC error: {}", source))]
    Ipc { source: ipc_communication::Error },

    #[snafu(display("IPC processing error(s): {:#?}", source))]
    Processing {
        source: ipc_communication::ParallelRunError,
    },
}

fn main() -> Result<(), Error> {
    let cfg = Config::from_args();
    let begin = Instant::now();

    let msg_size_distr = Normal::new(cfg.message_size_mean as f64, cfg.message_size_stddev as f64)
        .ok()
        .context(NormalDistr {
            stddev: cfg.message_size_stddev,
        })?;
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(cfg.seed);

    let Communication {
        client: ipc_client,
        processors,
        handle,
    } = communication::<Vec<u8>, Vec<u8>>(cfg.channels).context(Ipc)?;

    let run_until = Instant::now() + cfg.timeout;
    let clients: Vec<_> = (0..cfg.clients)
        .map(|_| {
            let seed = rng.gen();
            let rng = Xoshiro256PlusPlus::seed_from_u64(seed);
            run_client(
                ipc_client.clone(),
                rng,
                msg_size_distr,
                run_until,
                cfg.min_delay,
            )
        })
        .collect();
    spawn(move || {
        sleep(cfg.timeout);
        let _ = handle.stop();
    });

    processors
        .run_in_parallel(|_channel_id| {
            let mut rng = Xoshiro256PlusPlus::seed_from_u64(rng.gen());
            move |vec| {
                let sleep_for = rng.gen_range(Duration::from_nanos(100), Duration::from_micros(1));
                sleep(sleep_for);
                vec
            }
        })
        .context(Processing)?;

    let sent = clients
        .into_iter()
        .filter_map(|handle| {
            let id = handle.thread().id();
            match handle.join() {
                Err(_) => {
                    eprintln!("Thread #{:?} panicked", id);
                    None
                }
                Ok(Err(e)) => {
                    eprintln!("Thread #{:?} terminated with error: {}", id, e);
                    None
                }
                Ok(Ok(sent)) => Some(sent),
            }
        })
        .sum::<usize>();
    println!(
        "Sent {} bytes in total in {}",
        sent,
        humantime::format_duration(begin.elapsed())
    );

    Ok(())
}
