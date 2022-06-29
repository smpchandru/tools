use mqtt::{properties, AsyncClient};
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
#[derive(Clone, Debug, StructOpt)]
struct Request {
    /// publish topic name. REQUIRED
    #[structopt(long)]
    pub_topic: String,
    /// subscribe topic name. REQUIRED
    #[structopt(long)]
    resp_topic: String,
    /// correlation data. REQUIRED
    #[structopt(long)]
    corr_data: String,
    /// Message payload to publish. REQUIRED if --msg-file not there.
    #[structopt(long)]
    msg: Option<String>,
    /// Send message from content of file REQUIRED if --msg not there
    #[structopt(long)]
    msg_file: Option<String>,

    #[structopt(long,
                possible_values(&["0", "1", "2"]),
                default_value("1"))]
    qos: u8,
}
#[derive(Clone, Debug, StructOpt)]
struct Respond {
    /// publish topic name. REQUIRED
    #[structopt(long)]
    topic: String,
    /// Qos value to be used for sub/pub
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
    /// Subscribe and Publish on message
    Request(Request),
    /// Publish and then Subscribe for reponse
    Respond(Respond),
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
        Command::Request(ref req_args) => request(args.clone(), req_args.clone()).await,
        Command::Respond(ref resp_args) => respond(args.clone(), resp_args.clone()).await,
    }
}
async fn respond(args: Args, respond_args: Respond) {
    let mut cli = client_from_args(args.clone());
    println!("Connecting to mqtt host {}", args.host);
    cli.connect(get_connect_opts(args.clone())).await.unwrap();
    println!("Connected");
    let strm = cli.get_stream(1);
    println!("Subscribing {}", respond_args.topic.clone());
    cli.subscribe(respond_args.topic.clone(), respond_args.qos as i32)
        .await
        .unwrap();
    println!(
        "Successfully subscribed for topic {}",
        respond_args.topic.clone()
    );
    while let Ok(msg) = strm.recv().await {
        let cli = cli.clone();
        let _ = tokio::spawn(async move {
            let resp_msg: mqtt::Message;
            if let Some(msg) = msg {
                println!("received message {}", msg.topic());
                resp_msg = msg;
            } else {
                println!("received None");
                return;
            }
            println!(
                "Got message, with corr data {:?} Responding",
                String::from_utf8(
                    resp_msg
                        .properties()
                        .get_binary(mqtt::PropertyCode::CorrelationData)
                        .unwrap()
                )
                .unwrap()
            );

            let reps_topic = resp_msg
                .properties()
                .get_string(mqtt::PropertyCode::ResponseTopic)
                .unwrap();
            let msg = mqtt::MessageBuilder::new()
        .topic(reps_topic.to_owned())
        .qos(respond_args.qos as i32)
        .properties(
            properties!(
                mqtt::PropertyCode::CorrelationData => resp_msg.properties().get_binary(mqtt::PropertyCode::CorrelationData).unwrap()
            )
        )
        .payload(resp_msg.payload())
        .finalize();
            println!("Responding message to topic {}", reps_topic);
            if let Err(e) = cli.publish(msg).await {
                eprintln!("respond error {}", e)
            }
            println!("Responded successfully");
        });
    }
    cli.unsubscribe(respond_args.topic.clone()).await.unwrap();
}
async fn request(args: Args, req_args: Request) {
    let cli = client_from_args(args.clone());
    println!("Connecting to mqtt host {}", args.host);
    cli.connect(get_connect_opts(args.clone())).await.unwrap();
    println!("Connected");
    let mut cli2 = cli.clone();
    let (sub_tx, mut sub_rx) = tokio::sync::mpsc::channel(1);
    // let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(1);
    let req_args2 = req_args.clone();

    let sub_task = tokio::spawn(async move {
        let strm = cli2.get_stream(10);
        println!("Subscribing {}", req_args2.resp_topic.clone());
        cli2.subscribe(req_args2.resp_topic.clone(), req_args2.qos as i32)
            .await
            .unwrap();
        println!("Successfully subscribed for topic");
        sub_tx.send(true).await.unwrap();
        match strm.recv().await {
            Ok(msg) => {
                if let Some(msg) = msg {
                    println!(
                        "Received response {}, content {:?},correlation id [{}]",
                        msg.topic(),
                        String::from_utf8(msg.payload().to_owned()).unwrap(),
                        String::from_utf8(
                            msg.properties()
                                .get_binary(mqtt::PropertyCode::CorrelationData)
                                .unwrap()
                        )
                        .unwrap()
                    );
                } else {
                    println!("Received None response");
                }
            }
            Err(e) => eprintln!("Error while getting message from broker, {}", e),
        }
        sub_tx.send(true).await.unwrap();
        cli2.unsubscribe(req_args2.resp_topic.clone())
            .await
            .unwrap();
    });
    let req_args3 = req_args.clone();
    let msg = if let Some(ref file) = req_args2.msg_file {
        String::from_utf8(std::fs::read(file).unwrap()).unwrap()
    } else if let Some(msg) = req_args2.msg {
        msg
    } else {
        String::new()
    };
    if msg.is_empty() {
        println!("either --msg or --msg-file args missing");
        return;
    }
    let corr_id = req_args.corr_data.as_bytes().to_owned();

    let msg = mqtt::MessageBuilder::new()
        .topic(req_args.pub_topic.clone())
        .qos(req_args3.qos as i32)
        .properties(properties! {
            mqtt::PropertyCode::CorrelationData => corr_id,
            mqtt::PropertyCode::ResponseTopic => req_args.resp_topic
        })
        .payload(msg)
        .finalize();
    tokio::spawn(async move {
        // wait for response message subscriber to init
        sub_rx.recv().await;
        println!(
            "Requesting message to topic [{}], content [{:?}]",
            req_args3.pub_topic, req_args3.msg
        );
        if let Err(e) = cli.publish(msg).await {
            eprintln!("Error {}", e)
        }
        println!(
            "Waiting for response for request in resp topic [{}]",
            req_args3.resp_topic
        );
        sub_rx.recv().await;
    })
    .await
    .unwrap();
    sub_task.await.unwrap();
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
            println!("Publishing message to topic [{}]", pub_args.topic);
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
                "Topic : {} ,  Msg:{:?}, Correlation id {:?}",
                    sub_args.topic.join(","),
                String::from_utf8(msg.payload().to_owned()).unwrap(),
                String::from_utf8(
                    msg.properties()
                        .get_binary(mqtt::PropertyCode::CorrelationData)
                        .unwrap()
                )
                .unwrap()
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
