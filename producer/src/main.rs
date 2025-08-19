use anyhow::Result;
use clap::Parser;
use rand::distr::Alphanumeric;
use rand::{rng, Rng};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::time::Duration;
use tokio::pin;
use tokio::signal;
use tokio::time::{sleep, Instant};

#[derive(Parser, Debug)]
#[command(version, about = "Kafka producer for stream processing example", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "localhost:9095")]
    brokers: String,

    #[arg(short, long, default_value = "raw-topic")]
    topic: String,

    #[arg(short, long, default_value_t = 1)]
    events_per_second: u64,

    #[arg(short, long, default_value_t = 10)]
    max_producer_rate_with_print: u64,

    #[arg(short = 'l', long, default_value_t = 0)]
    message_value_lower_bound: u32,

    #[arg(short = 'u', long, default_value_t = 12)]
    message_value_upper_bound: u32,

    #[arg(short, long, default_value_t = 0.01)]
    corrupted_record_propability: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let delay = Duration::from_nanos(1_000_000_000 / args.events_per_second);

    println!(
        "Producing messages at a rate of {} events/second...",
        args.events_per_second
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &args.brokers)
        .set("message.timeout.ms", "5000")
        .create()?;

    let mut message_count = 0;
    // Create a Ctrl+C signal handler. This future will resolve when Ctrl+C is pressed.
    let shutdown_signal = signal::ctrl_c();
    // Pin the future to the stack so it can be used in `tokio::select!`.
    pin!(shutdown_signal);

    loop {
        let start_time = Instant::now();

        tokio::select! {
            _ = sleep(delay) => {
                let mut rng = rng();
                let payload = if rng.random_bool(args.corrupted_record_propability) {
                            rng.sample_iter(&Alphanumeric)
                                .take(20)
                                .map(char::from)
                                .collect::<String>()
                        } else {
                            rng.random_range(args.message_value_lower_bound..=args.message_value_upper_bound).to_string()
                        };
                let key = format!("Key {}", message_count);

                let record = FutureRecord::to(&args.topic)
                    .payload(&payload)
                    .key(&key);

                match producer.send(record, Timeout::Never).await {
                    Ok(_) => if args.events_per_second < args.max_producer_rate_with_print { println!("Message sent successfully: {}", payload)},
                    Err((e, _)) => eprintln!("Failed to send message: {:?}", e),
                }

                message_count += 1;

                let elapsed = start_time.elapsed();
                if elapsed < delay {
                    sleep(delay - elapsed).await;
                }
            },
            _ = &mut shutdown_signal => {
                println!("Graceful shutdown requested.");
                break;
            }
        }
    }

    // Flush any remaining messages to ensure they are sent before exiting.
    println!("Flushing remaining messages...");
    let _ = producer.flush(Duration::from_secs(10));
    println!("Producer finished.");

    Ok(())
}
