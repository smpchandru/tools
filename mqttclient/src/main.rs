use mqtt::AsyncClient;
use paho_mqtt as mqtt;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Clone, Debug, StructOpt)]
struct Publish {
    /// Topic name to publish to. REQUIRED
    #[structopt(long)]
    topic: String,

    /// Message payload to publish. REQUIRED if --msg-file not there.
    #[structopt(long)]
    msg: Option<String>,
    /// Send message from content of file REQUIRED if --msg not there
    #[structopt(long)]
    msg_file: Option<String>,

    /// Quality of service code to use
    #[structopt(long,
                possible_values(&["0", "1", "2"]),
                default_value("1"))]
    qos: u8,

    /// Send multiple copies of the message.
    #[structopt(long, default_value("1"))]
    repeats: i32,
}

#[derive(Clone, Debug, StructOpt)]
struct Subscribe {
    /// Topic names to subscribe to. REQUIRED
    #[structopt(long)]
    topic: Vec<String>,

    #[structopt(long,
                possible_values(&["0", "1", "2"]),
                default_value("1"))]
    qos: u8,
}
#[derive(StructOpt, Clone, Debug)]
enum Command {
    /// Publish meesage to broker
    Pub(Publish),
    /// Subscribe for messages from broker
    Sub(Subscribe),
}
#[derive(StructOpt, Clone, Debug)]
#[structopt(name = "basic")]
struct Args {
    #[structopt(subcommand)]
    cmd: Command,
    /// mqtt broker URL
    #[structopt(
        short = "h",
        long,
        env = "MQTT_URL",
        default_value = "ssl://mqtt.example.org:8883"
    )]
    host: String,
    /// mqtt client id
    #[structopt(long, env = "MQTT_CLIENT_ID", default_value = "mqtt-rust-client")]
    client_id: String,
    /// Make ssl connection.
    #[structopt(long)]
    ssl: bool,
    /// Ca pem file path.
    #[structopt(long, parse(from_os_str), default_value = "ca.pem")]
    ca_file: PathBuf,
    /// Cert pem file path.
    #[structopt(long, parse(from_os_str), default_value = "cert.pem")]
    cert_file: PathBuf,
    /// Key pem file path.
    #[structopt(long, parse(from_os_str), default_value = "key.pem")]
    key_file: PathBuf,
    /// User name(Optional).
    #[structopt(long)]
    user: Option<String>,
    /// Password(Optional).
    #[structopt(long)]
    pass: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::from_args();
    match args.cmd {
        Command::Pub(ref pub_args) => pub_msgs(args.clone(), pub_args.clone()).await,
        Command::Sub(ref sub_args) => sub_msgs(args.clone(), sub_args.clone()).await,
    }
}
async fn pub_msgs(args: Args, pub_args: Publish) {
    let cli = client_from_args(args.clone());
    println!("Connecting to mqtt host {}", args.host);
    cli.connect(get_connect_opts(args.clone())).await.unwrap();
    println!("Connected");
    let mut task = Vec::new();
    for _ in 1..pub_args.repeats + 1 {
        let cli = cli.clone();
        let pub_args = pub_args.clone();
        let msg = if let Some(ref file) = pub_args.msg_file {
            String::from_utf8(std::fs::read(file).unwrap()).unwrap()
        } else if let Some(msg) = pub_args.msg {
            msg
        } else {
            String::new()
        };
        if msg.is_empty() {
            println!("either --msg or --msg-file args missing");
            return;
        }
        let msg = mqtt::MessageBuilder::new()
            .topic(pub_args.topic.clone())
            .qos(pub_args.qos as i32)
            .payload(msg)
            .finalize();
        let s = tokio::spawn(async move {
            println!("Publishing message to topic {}", pub_args.topic);
            if let Err(e) = cli.publish(msg).await {
                eprintln!("Error {}", e)
            }
        });
        task.push(s);
    }
    for t in task {
        t.await.unwrap();
    }
}
async fn sub_msgs(args: Args, sub_args: Subscribe) {
    let mut cli = client_from_args(args.clone());
    println!("Connecting to {}", args.host);
    cli.connect(get_connect_opts(args.clone())).await.unwrap();
    println!("Connected...");
    tokio::spawn(async move {
        let strm = cli.get_stream(10);
        println!("Subscribing to topics {}", sub_args.topic.join(","));
        let _ = cli
            .subscribe_many(sub_args.topic.as_slice(), &[sub_args.qos as i32])
            .await
            .unwrap();
        println!("Subscribed waiting for messages");
        while let Some(msg) = strm.recv().await.unwrap() {
            println!(
                "Topic : {} ,  Msg:{:?}",
                sub_args.topic.join(","),
                msg.payload().to_owned()
            );
        }
    })
    .await
    .unwrap();
}

fn client_from_args(args: Args) -> AsyncClient {
    // Create a client & define connect options
    let create_opt = mqtt::CreateOptionsBuilder::new()
        .server_uri(&args.host)
        .client_id(args.client_id)
        .max_buffered_messages(100)
        .mqtt_version(mqtt::MQTT_VERSION_5)
        .finalize();
    mqtt::AsyncClient::new(create_opt).unwrap()
}
fn get_connect_opts(args: Args) -> mqtt::ConnectOptions {
    let mut conn_opts = mqtt::ConnectOptionsBuilder::new();
    if let Some(user) = args.user {
        conn_opts.user_name(user);
    }
    if let Some(pass) = args.pass {
        conn_opts.user_name(pass);
    }
    if args.ssl {
        let ssl_opts = mqtt::SslOptionsBuilder::new()
            .trust_store(args.ca_file)
            .unwrap()
            .key_store(args.cert_file)
            .unwrap()
            .private_key(args.key_file)
            .unwrap()
            .enable_server_cert_auth(false)
            .finalize();
        conn_opts.ssl_options(ssl_opts);
    }
    conn_opts.finalize()
}
