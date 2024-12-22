use clap::Parser;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

type Result<T> = anyhow::Result<T>;

#[derive(Debug, Parser)]
struct Opts {
    #[clap(index = 1)]
    database_url: String,

    #[clap(index = 2)]
    schema: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let pool = PgPoolOptions::new().connect(&opts.database_url).await?;
    let tables = tablenames(&pool, &opts.schema).await?;
    for table in tables {
        println!("{table}");
        let columns = table_columns(&pool, &table).await?;
        for col in columns {
            println!("  {}: {}", col.col_name, col.type_def);
        }
    }
    Ok(())
}

async fn tablenames(
    pool: &Pool<Postgres>,
    schema: impl AsRef<str>,
) -> Result<Vec<String>> {
    let tables: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT table_name
            FROM information_schema.tables
            WHERE table_schema=$1 AND table_type='BASE TABLE'
        "#,
    )
    .bind(schema.as_ref())
    .fetch_all(pool)
    .await?;
    Ok(tables.into_iter().map(|t| t.0).collect())
}

#[derive(Debug, sqlx::FromRow)]
struct ColumnInfo {
    column_name: String,
    ordinal_position: i32,
    is_nullable: String,
    udt_name: String,
    character_maximum_length: Option<i32>,
}

#[derive(Debug)]
struct ColumnDef {
    col_name: String,
    type_def: String,
}

async fn table_columns(
    pool: &Pool<Postgres>,
    tablename: impl AsRef<str>,
) -> Result<Vec<ColumnDef>> {
    let mut fields: Vec<ColumnInfo> = sqlx::query_as(
        r#"
        SELECT column_name, ordinal_position, is_nullable, udt_name, character_maximum_length
            FROM information_schema.columns
            WHERE table_name=$1
        "#,
    ).bind(tablename.as_ref()).fetch_all(pool).await?;
    fields.sort_by(|a, b| a.ordinal_position.cmp(&b.ordinal_position));
    Ok(fields
        .iter()
        .map(|field| column_definition(field))
        .collect())
}

fn column_definition(column: &ColumnInfo) -> ColumnDef {
    let (isvec, udt_name) = if column.udt_name.starts_with("_") {
        (true, &column.udt_name[1..])
    } else {
        (false, column.udt_name.as_str())
    };
    let mut type_def: String = match udt_name {
        "int4" => "DbInt4".into(),
        "int8" => "DbInt8".into(),
        "bpchar" => format!(
            "&[DbChar; {}]",
            column.character_maximum_length.unwrap_or_default(),
        ),
        "varchar" => "DbVarChar".into(),
        "text" => "DbText".into(),
        "timestamp" => "DbTimeStamp".into(),
        "jsonb" => "DbJson".into(),
        "uuid" => "DbUuid".into(),
        _ => format!("unsupported udt_name: {}", column.udt_name),
    };
    if isvec {
        type_def = format!("Vec<{}>", type_def);
    }
    if column.is_nullable == "YES" {
        type_def = format!("Option<{}>", type_def);
    }
    ColumnDef {
        col_name: column.column_name.clone(),
        type_def,
    }
}
