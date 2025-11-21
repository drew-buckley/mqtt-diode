use core::str;
use std::time::Duration;

use anyhow::{Context, Error, anyhow};
use argh::FromArgs;
use rumqttc::{Client, Connection, Event, Packet, QoS};

/// MQTT Diode
#[derive(FromArgs)]
struct Args {
    /// source broker address
    #[argh(
        option,
        short = 's',
        long = "source-broker",
        default = "String::from(\"127.0.0.1:1883\")"
    )]
    source_broker: String,

    /// destination broker address
    #[argh(
        option,
        short = 'd',
        long = "dest-broker",
        default = "String::from(\"127.0.0.1:1883\")"
    )]
    dest_broker: String,

    /// topic to forward
    #[argh(option, short = 't', long = "topic")]
    topics: Vec<String>,

    /// size of message queue buffers
    #[argh(option, short = 'b', long = "buffer-size", default = "64")]
    buffer_size: usize,

    /// format log messages to for syslog
    #[argh(switch, long = "syslog")]
    syslog: bool,
}

fn main() -> Result<(), Error> {
    let args: Args = argh::from_env();
    init_logging(args.syslog);

    let (source_host, source_port) = parse_address(&args.source_broker)
        .ok_or(anyhow!("invalid format for source broker address"))?;
    let (dest_host, dest_port) = parse_address(&args.dest_broker)
        .ok_or(anyhow!("invalid format for destination broker address"))?;

    log::info!(
        "Diode configuration: {} --> {}",
        args.source_broker,
        args.dest_broker
    );

    let (source_client, source_connection) = rumqttc::Client::new(
        rumqttc::MqttOptions::new("mqtt-diode-source", source_host, source_port),
        args.buffer_size,
    );

    for topic in args.topics {
        log::info!("Forwarding topic: {}", topic);
        source_client
            .subscribe(topic, QoS::AtMostOnce)
            .context("failed to subscribe to topic")?;
    }

    let (dest_client, dest_connection) = rumqttc::Client::new(
        rumqttc::MqttOptions::new("mqtt-diode-dest", dest_host, dest_port),
        args.buffer_size,
    );

    let source_join = std::thread::spawn(move || run_source(source_connection, dest_client));
    let dest_join = std::thread::spawn(move || run_dest(dest_connection));

    loop {
        if source_join.is_finished() {
            return source_join.join().unwrap();
        }

        if dest_join.is_finished() {
            return dest_join.join().unwrap();
        }

        std::thread::sleep(Duration::from_secs(1));
    }
}

fn run_source(mut source_connection: Connection, dest_client: Client) -> Result<(), Error> {
    for res in source_connection.iter() {
        let event = res.context("failed to receive source event")?;
        if let Event::Incoming(Packet::Publish(msg)) = event {
            log::debug!(
                "Forwarding {} {:?}",
                msg.topic,
                str::from_utf8(&msg.payload).unwrap_or("<BINARY>")
            );
            dest_client
                .publish(msg.topic, msg.qos, msg.retain, msg.payload)
                .context("failed to forward to destination client")?;
        } else {
            log::trace!("Source event: {:?}", event);
        }
    }

    Ok(())
}

fn run_dest(mut dest_connection: Connection) -> Result<(), Error> {
    for res in dest_connection.iter() {
        let event = res?;
        log::trace!("Destination event: {:?}", event);
    }
    Ok(())
}

fn parse_address<'a>(addr: &'a str) -> Option<(&'a str, u16)> {
    if addr.contains(":") {
        let mut s = addr.split(":");
        let host = s.next().unwrap();
        if let Ok(port) = s.next().unwrap().parse() {
            Some((host, port))
        } else {
            None
        }
    } else {
        Some((addr, 1883))
    }
}

fn init_logging(use_syslog: bool) {
    use std::io::Write;

    let mut log_builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));

    if use_syslog {
        log_builder.format(|buffer, record| {
            writeln!(buffer, "<{}>{}", record.level() as u8 + 2, record.args())
        });
    }
    log_builder.init();
}
