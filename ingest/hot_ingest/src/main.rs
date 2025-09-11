use futures_util::StreamExt;
use anyhow::Result;
use clap::Parser;
use rmp_serde::to_vec_named;
use serde::{Deserialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_tungstenite::connect_async;
use url::Url;

// async-nats imports
use async_nats::Client as NatsClient;

#[derive(Parser, Debug)]
struct Args {
    /// Mode: ws or file
    #[arg(long, default_value = "file")]
    mode: String,

    /// If mode=file, path to file (one JSON message per line)
    #[arg(long, default_value = "ingest/sample_feed.jsonl")]
    file: String,

    /// Websocket URL if mode=ws
    #[arg(long, default_value = "wss://example.invalid")]
    ws_url: String,

    /// NATS URL
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    /// subject prefix (e.g. mexc.raw)
    #[arg(long, default_value = "mexc.raw")]
    subj_prefix: String,

    /// Use JetStream publish (durable) and wait for server ACK
    #[arg(long, default_value_t = false)]
    jetstream: bool,
}

#[derive(Deserialize, Debug)]
struct RawMsg {
    symbol: String,
    price: Option<f64>,
    qty: Option<f64>,
    #[serde(flatten)]
    other: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    println!(
        "hot_ingest starting. mode={} nats={} ws_url={} jetstream={}",
        args.mode, args.nats_url, args.ws_url, args.jetstream
    );

    let nc: NatsClient = async_nats::connect(&args.nats_url).await?;
    println!("Connected to NATS at {}", args.nats_url);

    let js_ctx = if args.jetstream {
        Some(async_nats::jetstream::new(nc.clone()))
    } else {
        None
    };

    if args.mode == "file" {
        run_file_mode(&nc, js_ctx.as_ref(), &args.file, &args.subj_prefix).await?;
    } else {
        run_ws_mode(&nc, js_ctx.as_ref(), &args.ws_url, &args.subj_prefix).await?;
    }

    Ok(())
}

async fn run_file_mode(
    nc: &NatsClient,
    js_opt: Option<&async_nats::jetstream::Context>,
    file_path: &str,
    subj_prefix: &str,
) -> Result<()> {
    let f = File::open(file_path).await?;
    let reader = BufReader::new(f);
    let mut lines = reader.lines();
    let mut seq: u64 = 0;

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(&line)?;
        let symbol = v
            .get("symbol")
            .and_then(|s| s.as_str())
            .unwrap_or("UNKNOWN")
            .to_string();

        let envelope = serde_json::json!({
            "symbol": symbol,
            "price": v.get("price").and_then(|p| p.as_f64()),
            "qty": v.get("qty").and_then(|q| q.as_f64()),
            "seq_local": seq,
            "ts_local_us": SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64,
            "raw": v
        });

        let packed = to_vec_named(&envelope)?;
        let subj = format!(
            "{}.{}",
            subj_prefix,
            envelope
                .get("symbol")
                .and_then(|s| s.as_str())
                .unwrap_or("UNKNOWN")
        );

        if let Some(js) = js_opt {
            let t0 = std::time::Instant::now();
            match js.publish(subj.clone(), packed.into()).await {
                Ok(ack) => {
                    let elapsed_us = t0.elapsed().as_micros();
                    println!(
                        "JET_ACK seq_local={} subj={} ack={:?} ack_time_us={}",
                        seq, subj, ack, elapsed_us
                    );
                }
                Err(e) => {
                    eprintln!("JET_PUBLISH_ERROR seq_local={} subj={} err={}", seq, subj, e);
                }
            }
        } else {
            // CORRECTED: The typo std.time is now std::time
            let t0 = std::time::Instant::now();
            let packed_len = packed.len(); 
            nc.publish(subj.clone(), packed.into()).await?;
            nc.flush().await?;
            let t_us = t0.elapsed().as_micros();
            println!(
                "PUB seq_local={} subj={} bytes={} flush_time_us={}",
                seq,
                subj,
                packed_len,
                t_us
            );
        }

        seq += 1;
    }

    Ok(())
}

async fn run_ws_mode(
    nc: &NatsClient,
    js_opt: Option<&async_nats::jetstream::Context>,
    ws_url: &str,
    subj_prefix: &str,
) -> Result<()> {
    let url = Url::parse(ws_url)?;
    println!("Connecting to WS {}", url);
    let (ws_stream, _resp) = connect_async(url).await?;
    println!("Connected to WS.");
    let (_write, mut read) = ws_stream.split(); 
    let mut seq: u64 = 0;

    while let Some(msg) = read.next().await {
        let msg = msg?;
        let text = if msg.is_text() {
            msg.into_text()?
        } else if msg.is_binary() {
            String::from_utf8_lossy(msg.into_data().as_slice()).to_string()
        } else {
            continue;
        };

        let v: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("JSON parse error: {}", e);
                continue;
            }
        };

        let symbol = v
            .get("symbol")
            .and_then(|s| s.as_str())
            .unwrap_or("UNKNOWN")
            .to_string();

        let envelope = serde_json::json!({
            "symbol": symbol,
            "price": v.get("price").and_then(|p| p.as_f64()),
            "qty": v.get("qty").and_then(|q| q.as_f64()),
            "seq_local": seq,
            "ts_local_us": SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64,
            "raw": v
        });

        let packed = to_vec_named(&envelope)?;
        let subj = format!(
            "{}.{}",
            subj_prefix,
            envelope
                .get("symbol")
                .and_then(|s| s.as_str())
                .unwrap_or("UNKNOWN")
        );

        if let Some(js) = js_opt {
            let t0 = std::time::Instant::now();
            match js.publish(subj.clone(), packed.into()).await {
                Ok(ack) => {
                    let elapsed_us = t0.elapsed().as_micros();
                    println!(
                        "JET_ACK seq_local={} subj={} ack={:?} ack_time_us={}",
                        seq, subj, ack, elapsed_us
                    );
                }
                Err(e) => {
                    eprintln!("JET_PUBLISH_ERROR seq_local={} subj={} err={}", seq, subj, e);
                }
            }
        } else {
            // CORRECTED: The typo std.time is now std::time
            let t0 = std::time::Instant::now();
            let packed_len = packed.len(); 
            nc.publish(subj.clone(), packed.into()).await?;
            nc.flush().await?;
            let t_us = t0.elapsed().as_micros();
            println!(
                "PUB seq_local={} subj={} bytes={} flush_time_us={}",
                seq,
                subj,
                packed_len,
                t_us
            );
        }

        seq += 1;
    }

    Ok(())
}
