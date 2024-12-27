use chrono::Utc;
use clap::{ArgGroup, Parser};
use futures::StreamExt;
use lapin::types::AMQPValue;
use lapin::{options::*, types::FieldTable, Connection, ConnectionProperties};
use reqwest::Client;
use serde::Deserialize;
use std::fs::OpenOptions;
use std::io::Write;
use tokio::time::timeout;

#[derive(Parser)]
#[command(group(
    ArgGroup::new("output_format")
        .multiple(false)
        .required(true)
        .args(&["output_json"])
))]
struct Args {
    #[clap(short = 'a', long = "amqp-host", default_value = "localhost")]
    amqp_host: String,
    #[clap(short = 'p', long = "amqp-port", default_value = "5672")]
    amqp_port: u16,
    #[clap(short = 'm', long = "amqp-mgmt-port", default_value = "15672")]
    amqp_management_port: u16,
    #[clap(short = 'u', long = "amqp-user", default_value = "guest")]
    amqp_user: String,
    #[clap(short = 'P', long = "amqp-password", default_value = "guest")]
    amqp_password: String,
    #[clap(long = "prefetch", default_value_t = 1000)]
    prefetch_count: u16,
    #[clap(long = "json")]
    output_json: bool,
    #[clap(long = "stdout")]
    output_stdout: bool,
    #[clap(short = 's', long = "stream")]
    stream: Option<String>,
}

#[derive(Deserialize)]
struct QueueInfo {
    name: String,
    #[serde(rename = "type")]
    queue_type: String,
}

async fn list_streams(management_url: &str) -> Vec<String> {
    let client = Client::new();

    let response = client
        .get(management_url)
        .basic_auth("guest", Some("guest"))
        .send()
        .await
        .expect("Failed to fetch streams from RabbitMQ Management API");

    if response.status().is_success() {
        let queues: Vec<QueueInfo> = response
            .json()
            .await
            .expect("Failed to parse stream list JSON");
        queues
            .into_iter()
            .filter(|queue| queue.queue_type == "stream")
            .map(|queue| queue.name)
            .collect()
    } else {
        eprintln!("Failed to fetch streams: {}", response.status());
        vec![]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ts = Utc::now();

    let args = Args::parse();

    let addr = format!("amqp://{}:{}", args.amqp_host, args.amqp_port);

    let management_url = format!(
        "http://{}:{}/api/queues",
        args.amqp_host, args.amqp_management_port
    );

    // Connect to RabbitMQ
    let conn = Connection::connect(&addr, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");

    let channel = conn
        .create_channel()
        .await
        .expect("Failed to create channel");

    channel
        .basic_qos(args.prefetch_count, BasicQosOptions::default())
        .await
        .expect("Failed to set channel QoS");

    let streams = list_streams(&management_url).await;

    if let Some(stream) = &args.stream {
        if !streams.contains(stream) {
            eprintln!("Stream {} not found", stream);
            eprintln!("Available streams: {:?}", streams);
            std::process::exit(1);
        }
    } else if args.output_stdout {
        eprintln!("--stdout requires --stream");
        eprintln!("Available streams: {:?}", streams);
        std::process::exit(1);
    }

    for stream in streams.iter() {
        if args.output_stdout {
            if Some(stream) != args.stream.as_ref() {
                continue;
            }
        } else {
            println!("Consuming stream: {}", stream);
        }

        let mut file = if !args.output_stdout {
            let filename = format!("{}.{}.stream", stream, ts.to_rfc3339());
            Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&filename)
                    .expect("Unable to open file"),
            )
        } else {
            None
        };

        let mut consumer = channel
            .basic_consume(stream, "", BasicConsumeOptions::default(), {
                let mut args = FieldTable::default();
                args.insert(
                    "x-stream-offset".into(),
                    AMQPValue::LongString("first".into()),
                );
                args
            })
            .await
            .expect("Failed to start consuming stream");

        let mut i = 0;

        while let Ok(Some(delivery)) =
            timeout(std::time::Duration::from_millis(10_000), consumer.next()).await
        {
            i += 1;

            if !args.output_stdout {
                print!("\t processing event {i}:");
            }

            let delivery = delivery?;

            let payload = serde_json::from_slice::<serde_json::Value>(&delivery.data).unwrap();

            let event_name = payload
                .as_object()
                .map(|event| {
                    event
                        .get("event")
                        .map(|event_name| event_name.as_str().unwrap_or_default())
                })
                .flatten()
                .unwrap_or_default();

            if !args.output_stdout {
                println!(" {event_name}");
            }

            if let Some(file) = file.as_mut() {
                let output = serde_json::to_vec(&payload).unwrap();
                file.write_all(&output).expect("Unable to write data");
                file.write_all(b"\n").expect("Unable to write newline");
            } else {
                let output = serde_json::to_string(&payload).unwrap();
                println!("{output}");
            }

            delivery
                .ack(BasicAckOptions::default())
                .await
                .expect("Failed to ack send_webhook_event message");
        }
    }

    Ok(())
}
