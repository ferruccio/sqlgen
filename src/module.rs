use crate::{ColumnDef, Result};
use anyhow::anyhow;
use std::{
    fs::{create_dir, File},
    io::Write,
    path::PathBuf,
};

pub fn create_module_path(module_path: &PathBuf) -> Result<bool> {
    Ok(if let Err(err) = create_dir(module_path) {
        match err.kind() {
            std::io::ErrorKind::AlreadyExists => true,
            _ => return Err(anyhow!("create_module_path: {err}")),
        }
    } else {
        false
    })
}

pub fn create_table_src(
    module_path: &PathBuf,
    table: impl AsRef<str>,
    columns: &[ColumnDef],
) -> Result<()> {
    let table = table.as_ref().to_string();
    let table_src = PathBuf::from_iter([
        module_path,
        &PathBuf::from(table.clone() + ".rs"),
    ]);
    let mut file = File::create(table_src)?;
    let rec_type = record_type(&table);
    file.write_all(b"use super::types::*;\n\n#[derive(Debug)]\n")?;
    file.write_all(format!("pub struct {rec_type} {{\n").as_bytes())?;
    for column in columns {
        file.write_all(
            format!(
                "    pub {col_name}: {type_def},\n",
                col_name = column.col_name,
                type_def = column.type_def
            )
            .as_bytes(),
        )?;
    }
    file.write_all(b"}\n")?;
    Ok(())
}

pub fn create_module(module_path: &PathBuf, tables: &[String]) -> Result<()> {
    let mod_src = PathBuf::from_iter([module_path, &PathBuf::from("mod.rs")]);
    let mut file = File::create(mod_src)?;
    file.write_all(b"mod types;\n\n")?;
    file.write_all(b"pub use types::*;\n\n")?;
    for table in tables {
        file.write_all(format!("mod {table};\n").as_bytes())?;
    }
    file.write_all(b"\n")?;
    for table in tables {
        file.write_all(format!("pub use {table}::*;\n").as_bytes())?;
    }
    Ok(())
}

pub fn create_module_types(module_path: &PathBuf) -> Result<()> {
    let mod_types =
        PathBuf::from_iter([module_path, &PathBuf::from("types.rs")]);
    let mut file = File::create(mod_types)?;
    file.write_all(
        br#"pub type Connection = sqlx::Pool<sqlx::Postgres>;

pub type DbInt4 = i32;
pub type DbInt8 = i64;
pub type DbText = String;
pub type DbVarChar = String;
pub type DbChar = String;
pub type DbTimeStamp = sqlx::types::time::PrimitiveDateTime;
pub type DbUuid = sqlx::types::Uuid;
pub type DbJson = sqlx::types::JsonValue;
"#,
    )?;
    Ok(())
}

fn record_type(table_name: impl AsRef<str>) -> String {
    let mut output = String::new();
    let mut capitalize = true;
    for ch in table_name.as_ref().chars() {
        if ch.is_alphabetic() {
            output.push(if capitalize {
                ch.to_ascii_uppercase()
            } else {
                ch
            });
            capitalize = false;
        } else {
            capitalize = true;
        }
    }
    format!("Db{output}Rec")
}
