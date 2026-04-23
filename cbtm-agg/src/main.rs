use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};
use std::env;
use std::time::Duration;
use serde::{Deserialize, Serialize};
use jsonwebtoken::{decode, DecodingKey, Validation};
mod db;

static CYCLE_TIME_SECONDS: OnceLock<i64> = OnceLock::new();
fn recent_ping_window_seconds() -> i64 {
    *CYCLE_TIME_SECONDS.get_or_init(|| {
        env::var("CYCLE_TIME_SECONDS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(180)
    })
}

static JWT_SECRET_KEY: OnceLock<String> = OnceLock::new();
fn jwt_secret_key() -> &'static str {
    JWT_SECRET_KEY.get_or_init(|| {
        env::var("JWT_SECRET_KEY")
            .unwrap_or_else(|_| "defaultsecretkey".to_string())
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct AggregateCountMessage {
    token: String,
    counts: HashMap<i32, i32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    company: String,
    exp: usize,
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello, World!")
}

async fn flush_aggregate_counts(
    aggregate_queue: web::Data<Mutex<VecDeque<AggregateCountMessage>>>,
    connection: sqlx::PgPool,
) {
    loop {
        // Sleep for the cycle time duration
        actix_web::rt::time::sleep(Duration::from_secs(recent_ping_window_seconds() as u64)).await;

        // Flush the aggregate counts to the database
        if let Ok(mut agg_counts) = aggregate_queue.lock() {
            println!("Flushing aggregate counts: {:?}", *agg_counts);
            while let Some(counts) = agg_counts.front() {
                for (poi_id, count) in counts.counts.iter() {
                    println!("Flushing count for POI {}: {}", poi_id, count);
                    // Here you would call your database function to update the aggregate count for the POI
                   db::insert_aggregate_counts(&connection, poi_id, count).await.expect("Failed to insert aggregate counts into the database");
                }
                agg_counts.pop_front();
            }
        }
    }
}

#[post("/submit-aggregate-count")]
async fn submit_aggregate_count(
    aggregate_queue: web::Data<Mutex<VecDeque<AggregateCountMessage>>>,
    req_body: web::Json<AggregateCountMessage>,
) -> impl Responder {
    let counts = req_body.into_inner();

    // Validate the JWT token before accepting the aggregate counts
    let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.validate_exp = false; // Disable expiration validation for testing purposes, enable in production
    let token = counts.token.clone();
    let decoding_key = DecodingKey::from_secret(jwt_secret_key().as_ref());

        match decode::<Claims>(token, &decoding_key, &validation) {
            Ok(token_data) => {
                println!("Token is valid. Claims: {:?}", token_data.claims);
                println!("Received aggregate counts: {:?}", counts.counts);
            }
            Err(err) => {
                println!("Token validation failed: {:?}", err);
                return HttpResponse::Unauthorized().body("Invalid JWT token");
            }
        }

    if let Ok(mut agg_counts) = aggregate_queue.lock() {
        agg_counts.push_back(counts);
        println!("Received new aggregate counts, added to queue. Queue length: {}", agg_counts.len());
    } else {
        println!("Failed to acquire lock on aggregate queue");
        return HttpResponse::InternalServerError().body("Failed to acquire lock on aggregate queue");
    }
    HttpResponse::Ok().body("Aggregate counts received")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting the aggregator server...");
    dotenv().ok(); // Load environment variables from .env file
    let db_connection_string = std::env::var("DB_CONNECTION_STRING")
        .expect("DB_CONNECTION_STRING must be set in environment or .env");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_connection_string)
        .await
        .expect("Failed to connect to the database");
    println!("Database connection pool created successfully.");

    let aggregate_queue: web::Data<Mutex<VecDeque<AggregateCountMessage>>> = web::Data::new(Mutex::new(VecDeque::<AggregateCountMessage>::new()));
    actix_web::rt::spawn(flush_aggregate_counts(
        aggregate_queue.clone(),
        pool.clone(),
    ));

    let server = HttpServer::new(move || {
        App::new().app_data(web::Data::new(pool.clone()))
            .app_data(aggregate_queue.clone())
            .service(hello)
    })
    .bind("localhost:5174")?
    .run();

    server.await
}