use taos::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dsn = "taos://localhost:6030";
    let builder = TaosBuilder::from_dsn(dsn)?;

    let taos = builder.build()?;

// ANCHOR: create_db_and_table
let db = "power";
// create database
taos.exec_many([
    format!("CREATE DATABASE IF NOT EXISTS `{db}`"),
    format!("USE `{db}`"),
])
.await?;
println!("Create database power successfully.");

// create super table
taos.exec_many([
    "CREATE TABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) \
        TAGS (`groupid` INT, `location` BINARY(24))",
]).await?;
println!("Create stable meters successfully.");

// ANCHOR_END: create_db_and_table

// ANCHOR: insert_data
let inserted = taos.exec("INSERT INTO " +
"power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') " +
"VALUES " +
"(NOW + 1a, 10.30000, 219, 0.31000) " +
"(NOW + 2a, 12.60000, 218, 0.33000) " +
"(NOW + 3a, 12.30000, 221, 0.31000) " +
"power.d1002 USING power.meters TAGS(3, 'California.SanFrancisco') " +
"VALUES " +
"(NOW + 1a, 10.30000, 218, 0.25000) ").await?;

println!("inserted: {} rows to power.meters successfully.", inserted);
// ANCHOR_END: insert_data

// ANCHOR: query_data
// query data, make sure the database and table are created before
let mut result = taos.query("SELECT ts, current, location FROM power.meters limit 100").await?;

for field in result.fields() {
    println!("got field: {}", field.name());
}

let mut rows = result.rows();
let mut nrows = 0;
while let Some(row) = rows.try_next().await? {
    for (col, (name, value)) in row.enumerate() {
        println!(
            "[{}] got value in col {} (named `{:>8}`): {}",
            nrows, col, name, value
        );
    }
    nrows += 1;
}
// ANCHOR_END: query_data

// ANCHOR: query_with_req_id
let result = taos.query_with_req_id("SELECT ts, current, location FROM power.meters limit 1", 1).await?;
// ANCHOR_END: query_with_req_id

}
