// use postgres::{Client, NoTls, Error};
use tokio_postgres::{NoTls, Error};
use std::collections::HashMap;

struct Shelf {
    _id: i32,
    book: String,
    collection: String
}

struct Book {
    _id: i32,
    title: String,
    author_id: i32
}

#[derive(Debug)]
struct Author {
    _id: i32,
    name: String,
    country: String
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    
    let (mut client, mut connection) = tokio_postgres::connect("postgres://khal:12345@localhost:5432/rs_library", NoTls).await?;
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e)
        }
    });
    
    client.batch_execute("
        CREATE TABLE IF NOT EXISTS shelf(
            id          SERIAL PRIMARY KEY,
            section     VARCHAR NOT NULL
        )
    ").await?;

    client.batch_execute("
        CREATE TABLE IF NOT EXISTS author(
            id          SERIAL PRIMARY KEY,
            name        VARCHAR NOT NULL,
            country     VARCHAR NOT NULL
        )
    ").await?;

    client.batch_execute("
        CREATE TABLE IF NOT EXISTS book(
            id          SERIAL PRIMARY KEY,
            title       VARCHAR NOT NULL,
            author_id   INTEGER NOT NULL REFERENCES author(id),
            shelf_id    INTEGER NOT NULL REFERENCES shelf(id)
        )
    ").await?;

    let mut authors = HashMap::new();

    authors.insert(String::from("Pascal Akunne"), "Nigeria");
    authors.insert(String::from("Alec Sandler"), "England");
    authors.insert(String::from("Matt Simon"), "USA");

    for (key, val) in &authors{
        let author = Author{
            _id: 0,
            name: key.to_string(),
            country: val.to_string()
        };

        // query the author name
        let data = client.query(
            "SELECT * FROM author WHERE name = $1", 
            &[&author.name]
        ).await?;

        //insert author if not exists; to avoid duplicate
        if data.len() == 0 {
            client.execute(
                "INSERT INTO author(name, country) VALUES ($1, $2)",
                &[&author.name, &author.country]
            ).await?;
        }
    }

    client.execute(
        "INSERT INTO shelf(section) VALUES ($1), ($2), ($3)",
        &[&"History", &"Biology", &"Physics"]
    ).await?;


    let author_rows = client.query("SELECT id, name, country FROM author", &[]).await?;

    let shelf_rows = client.query("SELECT id, section FROM shelf", &[]).await?;


    let author_collection: Vec<Author> = author_rows.iter().map(|row| {
        Author {
            _id: row.get(0),
            name: row.get(1),
            country: row.get(2)
        }
    }).collect();

    for author in &author_collection{

        println!("{:?}", author);

    }

    // for shelf_row in shelf_rows{
    //     let shelf_id: i32 = shelf_row.get(0);
    //     let shelf_section: String = shelf_row.get(1);

    //     println!("\n{}, {}", shelf_id, shelf_section)
    // }

    // println!("Author: {:#?}", result.get(0).unwrap());
    // println!("Shelf: {:#?}", shelfs);

    Ok(())
}
