use chrono::{Utc};
use uuid::Uuid;
use crate::db;
use geo::algorithm::Contains;
use std::collections::VecDeque;
use std::env;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, serde::Deserialize)]
pub struct UserPing {
    pub id: i64,
    bssid: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub timestamp: i64,
    user_id: String
}

const RECENT_PING_MAX_ENTRIES: usize = 100;

#[derive(Debug, Clone, Copy)]
struct RecentPing {
    user_id: i64,
    timestamp: i64,
}

static RECENT_PINGS: OnceLock<Mutex<VecDeque<RecentPing>>> = OnceLock::new();
static RECENT_PING_WINDOW_SECONDS: OnceLock<i64> = OnceLock::new();

fn recent_ping_window_seconds() -> i64 {
    *RECENT_PING_WINDOW_SECONDS.get_or_init(|| {
        env::var("CYCLE_TIME_SECONDS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(180)
    })
}

fn recent_pings() -> &'static Mutex<VecDeque<RecentPing>> {
    RECENT_PINGS.get_or_init(|| Mutex::new(VecDeque::with_capacity(RECENT_PING_MAX_ENTRIES)))
}

fn prune_expired(pings: &mut VecDeque<RecentPing>, now: i64) {
    while let Some(front) = pings.front() {
        if now - front.timestamp > recent_ping_window_seconds() {
            pings.pop_front();
        } else {
            break;
        }
    }
}

fn is_recent_ping_allowed(user_id: i64, now: i64) -> bool {
    let Ok(mut pings) = recent_pings().lock() else {
        println!("Failed to lock recent pings list");
        return false;
    };

    // Remove stale entries first to keep lookup fast and bounded by recent activity.
    prune_expired(&mut pings, now);

    if let Some(last_ping) = pings.iter().rev().find(|entry| entry.user_id == user_id) {
        if now - last_ping.timestamp < recent_ping_window_seconds() {
            println!(
                "Rejecting ping for user {}: last ping {} seconds ago",
                user_id,
                now - last_ping.timestamp
            );
            return false;
        }
    }

    true
}

fn record_recent_ping(user_id: i64, now: i64) {
    let Ok(mut pings) = recent_pings().lock() else {
        println!("Failed to lock recent pings list");
        return;
    };

    prune_expired(&mut pings, now);

    if pings.len() >= RECENT_PING_MAX_ENTRIES {
        pings.pop_front();
    }
    pings.push_back(RecentPing {
        user_id,
        timestamp: now,
    });

}

async fn verify_user_ping(ping: &UserPing, connection: &sqlx::PgPool) -> bool {
    // Verify that we have a valid user ID and that the timestamp is within the last 10 minutes
    let now = Utc::now().timestamp();
    if (now - ping.timestamp) > 600 {
        println!("Ping timestamp is too old: {}", ping.timestamp);
        return false;
    }
    if Uuid::parse_str(&ping.user_id).is_err() {
        println!("Invalid user ID: {}", ping.user_id);
        return false;
    }
    if !is_recent_ping_allowed(ping.id, now) {
        return false;
    }
    let verified_user = db::verify_user_auth_id(connection, ping.id, ping.user_id.clone()).await;
    println!("Verified user auth ID for ping ID {}: {:?}", ping.id, verified_user);
    if verified_user.is_err() || !verified_user.unwrap() { 
        println!("User ID does not match auth ID for ping ID: {}", ping.id);
        return false;
    }
    // Check if the location is somewhere in the DMV (roughly lat 38.5 to 39.5, lon -77.5 to -76.5)
    if ping.latitude < 38.5 || ping.latitude > 39.5 || ping.longitude < -77.5 || ping.longitude > -76.5 {
        println!("Ping location is out of bounds: ({}, {})", ping.latitude, ping.longitude);
        return false;
    }

    // Only accepted pings are written to the recent-ping cooldown list.
    record_recent_ping(ping.id, now);
    true
}

fn match_ping_to_poi<'a>(ping: &UserPing, pois: &'a [db::POI]) -> Option<&'a db::POI> {
    // In the future, we will use geohashing for the local db to quickly narrow down candidate POIs, but for now we'll just do a brute force search
    for poi in pois {
        // Check if the ping location is within the POI bounds
        if poi.bounds.contains(&geo_types::Point::new(ping.longitude, ping.latitude)) {
            return Some(poi);
        };
    }
    None
}

pub async fn process_user_ping(
    ping: &UserPing,
    pois: &[db::POI],
    connection: &sqlx::PgPool,
) -> Result<Option<i32>, sqlx::Error> {
    // First verify the ping
    if !verify_user_ping(ping, connection).await {
        return Ok(None);
    }

    // If the ping is valid, try to match it to a POI
    let matched = match_ping_to_poi(ping, pois).map(|poi| poi.id);
    if let Some(poi_id) = matched {
        print!("Matched ping {} to POI {}", ping.id, poi_id);
    }
    Ok(matched)
}