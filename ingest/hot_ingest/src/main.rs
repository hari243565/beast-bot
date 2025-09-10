use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use async_nats::Client; // CORRECTED: Use the Client from async-nats
use rmp_serde::to_vec_named;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_tungstenite::connect_async;
use url::Url;

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
}

#[derive(Serialize, Deserialize, Debug)]
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
    println!("hot_ingest starting. mode={} nats={} ws/url={}", args.mode, args.nats_url, args.ws_url);

    // CORRECTED: Connect to NATS using the async-nats crate
    let nc = async_nats::connect(&args.nats_url).await?;
    println!("Connected to NATS at {}", args.nats_url);

    if args.mode == "file" {
        run_file_mode(&nc, &args.file, &args.subj_prefix).await?;
    } else {
        run_ws_mode(&nc, &args.ws_url, &args.subj_prefix).await?;
    }

    Ok(())
}

// CORRECTED: Function signature now uses the correct Client type
async fn run_file_mode(nc: &Client, file_path: &str, subj_prefix: &str) -> Result<()> {
    let f = File::open(file_path).await?;
    let reader = BufReader::new(f);
    let mut lines = reader.lines();
    let mut seq: u64 = 0;
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() { continue; }
        // parse as JSON
        let v: serde_json::Value = serde_json::from_str(&line)?;
        let symbol = v.get("symbol").and_then(|s| s.as_str()).unwrap_or("UNKNOWN").to_string();

        // build RawMsg (partial)
        let raw = RawMsg {
            symbol: symbol.clone(),
            price: v.get("price").and_then(|p| p.as_f64()),
            qty: v.get("qty").and_then(|q| q.as_f64()),
            other: v.clone(),
        };

        // stamp local monotonic-like timestamp (use SystemTime as practical)
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;

        // prepare envelope
        let envelope = serde_json::json!({
            "symbol": raw.symbol,
            "price": raw.price,
            "qty": raw.qty,
            "seq_local": seq,
            "ts_local_us": now,
            "raw": raw.other
        });

        // encode MessagePack (rmp-serde)
        let packed = to_vec_named(&envelope)?;

        // publish to subject mexc.raw.<symbol>
        let subj = format!("{}.{}", subj_prefix, envelope.get("symbol").and_then(|s| s.as_str()).unwrap_or("UNKNOWN"));
        
        // CORRECTED: The publish method takes the payload by value or convertible type
        match nc.publish(subj.clone(), packed.into()).await {
            Ok(_) => {
                println!("PUB seq={} subj={} bytes={}", seq, subj, to_vec_named(&envelope)?.len());
            }
            Err(e) => {
                eprintln!("Publish error: {}", e);
            }
        }

        seq += 1;
    }
    Ok(())
}

// CORRECTED: Function signature now uses the correct Client type
async fn run_ws_mode(nc: &Client, ws_url: &str, subj_prefix: &str) -> Result<()> {
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

        // try parse as JSON
        let v: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("JSON parse error: {}", e);
                continue;
            }
        };

        let symbol = v.get("symbol").and_then(|s| s.as_str()).unwrap_or("UNKNOWN").to_string();
        let raw = RawMsg {
            symbol: symbol.clone(),
            price: v.get("price").and_then(|p| p.as_f64()),
            qty: v.get("qty").and_then(|q| q.as_f64()),
            other: v.clone(),
        };

        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;
        let envelope = serde_json::json!({
            "symbol": raw.symbol,
            "price": raw.price,
            "qty": raw.qty,
            "seq_local": seq,
            "ts_local_us": now,
            "raw": raw.other
        });

        let packed = to_vec_named(&envelope)?;
        let subj = format!("{}.{}", subj_prefix, envelope.get("symbol").and_then(|s| s.as_str()).unwrap_or("UNKNOWN"));
        
        // CORRECTED: The publish method takes the payload by value or convertible type
        match nc.publish(subj.clone(), packed.into()).await {
            Ok(_) => println!("PUB seq={} subj={} bytes={}", seq, subj, to_vec_named(&envelope)?.len()),
            Err(e) => eprintln!("Publish error: {}", e),
        }
        seq += 1;
    }
    Ok(())
}
