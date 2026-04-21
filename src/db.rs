use geozero::wkb;
use geozero::ToGeo;
use sqlx::postgres::PgRow;
use sqlx::Row;
use geo::{Polygon, Point};
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct POI {
    pub id: i32,
    pub name: String,
    pub location: Point<f64>,
    pub bounds: Polygon<f64>,
    pub updated: DateTime<Utc>,
}

impl From<&PgRow> for POI {
    fn from(row: &PgRow) -> Self {
        let location = row.get::<Vec<u8>, _>("location");
        let bounds = row.get::<Vec<u8>, _>("bounds");
        let parsed_location = match wkb::Wkb(&location).to_geo().expect("Failed to extract geometry for centroid") {
            geo_types::Geometry::Point(point) => point,
            other => panic!("Expected centroid to be a Point, got {other:?}"),
        };
        let parsed_bounds = match wkb::Wkb(&bounds).to_geo().expect("Failed to extract geometry for bounds") {
            geo_types::Geometry::Polygon(polygon) => polygon,
            other => panic!("Expected bounds to be a Polygon, got {other:?}"),
        };

        Self {
            id: row.get("id"),
            name: row.get("name"),
            location: parsed_location,
            bounds: parsed_bounds,
            updated: row.get("updated"),
        }
    }
}

pub async fn get_pois(connection: &sqlx::PgPool) -> Result<Vec<POI>, sqlx::Error> {
    // When we pass a connection pool fetch_all or execute auto pull from the pool
    let rows = sqlx::query(
        "SELECT id, name, gis.st_asbinary(location) AS location, gis.st_asbinary(bounds) AS bounds, updated FROM pois"
    )
    // we map each row to a POI struct using the From trait we implemented, then return the vector of POIs
    .fetch_all(connection)
    .await?;

    let pois : Vec<POI> = rows.iter().map(POI::from).collect();

    println!("Fetched {} POIs from the database", pois.len());
    println!("Sample POI: {:?}", pois.first());
    
    Ok(pois)
}

//Helper that checks whether a user's (found by record id) matches their auth id
pub async fn verify_user_auth_id(connection: &sqlx::PgPool, user_id: i64, passed_auth_id: String) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(
        "SELECT auth_id FROM users WHERE id = $1"
    )
    .bind(user_id)
    .fetch_one(connection)
    .await?;

    let auth_id: sqlx::types::Uuid = row.get("auth_id");
    Ok(auth_id.to_string() == passed_auth_id)
}