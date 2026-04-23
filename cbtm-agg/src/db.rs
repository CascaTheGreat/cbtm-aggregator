pub async fn insert_aggregate_counts(connection: &sqlx::PgPool, poi_id: &i32, count: &i32) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE pois SET aggregate_count = $2 WHERE id = $1 "
    )
    .bind(poi_id)
    .bind(count)
    .execute(connection)
    .await?;
    Ok(())
}