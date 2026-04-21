use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use dotenvy::dotenv;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::env;
use std::time::Duration;
mod db;
mod packet;

#[derive(Deserialize)]
struct EchoRequest {
    message: String,
}

struct QueueItem {
    ping: packet::UserPing,
    timestamp: i64,
}
static CYCLE_TIME_SECONDS: OnceLock<i64> = OnceLock::new();
fn recent_ping_window_seconds() -> i64 {
    *CYCLE_TIME_SECONDS.get_or_init(|| {
        env::var("CYCLE_TIME_SECONDS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(180)
    })
}

// Routes
#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello, World!")
}

#[post("/echo")]
async fn echo(req_body: web::Json<EchoRequest>) -> impl Responder {
    HttpResponse::Ok().body(req_body.message.clone())
}

// Take a ping from the user, add it to the processing queue, and return a response immediately
#[post("/ping")]
async fn ping(
    ping_queue: web::Data<Mutex<VecDeque<QueueItem>>>,
    req_body: web::Json<packet::UserPing>,
) -> impl Responder {
    let ping = req_body.into_inner();
    let timestamp = ping.timestamp;
    if let Ok(mut queue) = ping_queue.lock() {
        queue.push_back(QueueItem { ping, timestamp });
        HttpResponse::Ok().body("Ping received")
    } else {
        HttpResponse::InternalServerError().body("Queue unavailable")
    }
}

async fn process_queue_and_aggregate(
    ping_queue: web::Data<Mutex<VecDeque<QueueItem>>>,
    agg_poi_count: web::Data<Mutex<HashMap<i32, i32>>>,
    pois: Vec<db::POI>,
    connection: sqlx::PgPool,
) {
    loop {
        let maybe_item = {
            if let Ok(mut queue) = ping_queue.lock() {
                queue.pop_front()
            } else {
                print!("Failed to acquire lock on ping queue");
                None
            }
        };

        if let Some(item) = maybe_item {
            match packet::process_user_ping(&item.ping, &pois, &connection).await {
                Ok(Some(poi_id)) => {
                    if let Ok(mut counts) = agg_poi_count.lock() {
                        *counts.entry(poi_id).or_insert(0) += 1;
                        println!("Incremented POI {poi_id} from ping {} at {}", item.ping.id, item.timestamp);
                    } else {
                        print!("Failed to acquire lock on POI count map");
                    }
                }
                Ok(None) => {
                    println!("Ping {} did not match a verified POI", item.ping.id);
                }
                Err(e) => {
                    print!("Error processing ping ID {}: {}", item.ping.id, e);
                }
            }
        } else {
            actix_web::rt::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

async fn flush_aggregate_counts(agg_poi_count: web::Data<Mutex<HashMap<i32, i32>>>, endpoint: String) {
    let client = reqwest::Client::new();

    loop {
        let sleep_seconds = recent_ping_window_seconds().max(1) as u64;
        actix_web::rt::time::sleep(Duration::from_secs(sleep_seconds)).await;
        println!("Flushing aggregated POI counts to endpoint...");

        let snapshot = {
            if let Ok(mut counts) = agg_poi_count.lock() {
                if counts.is_empty() {
                    None
                } else {
                    Some(std::mem::take(&mut *counts))
                }
            } else {
                print!("Failed to acquire lock on POI count map for flush");
                None
            }
        };

        if let Some(payload) = snapshot {
            match client.post(&endpoint).json(&payload).send().await {
                Ok(_) => println!("Flushed POI counts to dummy endpoint"),
                Err(e) => print!("Failed to flush POI counts: {}", e),
            }
        }
    }
}

// Main function to start the server
#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting the server...");
    dotenv().ok(); // Load environment variables from .env file
    let db_connection_string = env::var("DB_CONNECTION_STRING")
        .expect("DB_CONNECTION_STRING must be set in environment or .env");

    // Create the database connection pool
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_connection_string)
        .await
        .expect("Failed to connect to the database");
    println!("Database connection pool created successfully.");

    // TODO: Spin up a cached local database and sync with the remote one, then use that for queries instead of the remote one
    let pois = db::get_pois(&pool).await.expect("Failed to fetch POIs from the database");
    println!("Fetched {} POIs from the database", pois.len());

    let ping_queue = web::Data::new(Mutex::new(VecDeque::<QueueItem>::new()));
    let agg_poi_count = web::Data::new(Mutex::new(HashMap::<i32, i32>::new())); // POI ID -> count
    actix_web::rt::spawn(process_queue_and_aggregate(
        ping_queue.clone(),
        agg_poi_count.clone(),
        pois,
        pool.clone(),
    ));

    actix_web::rt::spawn(flush_aggregate_counts(
        agg_poi_count.clone(),
        "https://httpbin.org/post".to_string(),
    ));

    // Init the server with the database pool and routes
    let server = HttpServer::new(move || {
        App::new().app_data(web::Data::new(pool.clone()))
            .app_data(ping_queue.clone())
            .app_data(agg_poi_count.clone())
            .service(hello)
            .service(echo)
            .service(ping)
    })
    .bind("localhost:5173")?
    .run();

    server.await
}