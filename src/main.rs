// use postgres::{Client, NoTls, Error};
use tokio_postgres::{NoTls, Error};
use std::collections::HashMap;
use serde::Serialize;
use dotenv::dotenv;
use std::env;


#[derive(Serialize, Debug)]
struct Shelf {
    _id: i32,
    section: String
}


#[derive(Serialize, Debug)]
struct Book {
    _id: i32,
    title: String,
    author_id: i32,
    shelf_id: i32
}

#[derive(Serialize, Debug)]
struct Author {
    _id: i32,
    name: String,
    country: String
}


#[tokio::main]
async fn main() -> Result<(), Error> {

    // Load environment variable
    dotenv().ok();

    let db_username = env::var("DB_USERNAME").expect("Database username variable is not found");
    let db_password = env::var("DB_PASSWORD").expect("Database password variable is not found");
    let db_name = env::var("DB_NAME").expect("Database name variable is not found");
    let db_hostname = env::var("DB_HOSTNAME").expect("Database hostname variable is not found");
    let db_port = env::var("DB_PORT").expect("Database port variable is not found");


    // Shelf table sections
    let sections = ["History", "Tech", "Physics"];

    let connection_url = format!("postgres://{db_username}:{db_password}@{db_hostname}:{db_port}/{db_name}");
    
    let (client, connection) = tokio_postgres::connect(&connection_url, NoTls).await?;
    
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

    let mut authors_map = HashMap::new();

    authors_map.insert(String::from("Pascal Akunne"), "Nigeria");
    authors_map.insert(String::from("Alec Sandler"), "England");
    authors_map.insert(String::from("Matt Simon"), "USA");

    for (key, val) in &authors_map{
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


    // loop through the sections array
    for section in sections{

        // query the shelf table to see if the section name already exists
        let data = client.query(
            "SELECT * FROM shelf WHERE section = $1",
            &[&section]
        ).await?;

        // insert if it does not exist
        if data.len() == 0 {

            client.execute(
                "INSERT INTO shelf(section) VALUES ($1)",
                &[&section]
            ).await?;
        }
    }


    let author_rows = client.query("SELECT id, name, country FROM author", &[]).await?;
    let shelf_rows = client.query("SELECT id, section FROM shelf", &[]).await?;


    let authors: Vec<Author> = author_rows.iter().map(|row| {
        Author {
            _id: row.get(0),
            name: row.get(1),
            country: row.get(2)
        }
    }).collect();

    for author in &authors{

        println!("AUTHORS: {:?}", author);

    }

    let shelves: Vec<Shelf> = shelf_rows.iter().map(|row| {
        Shelf{
            _id: row.get(0),
            section: row.get(1)
        }
    }).collect();

    for shelf in &shelves{
        println!("SHELF: {:?}", shelf);
    }

    client.execute(
        "INSERT INTO book(title, author_id, shelf_id) VALUES
        ($1, $2, $3),
        ($4, $5, $6),
        ($7, $8, $9)
        ",
        
        &[
            &"JavaScript to Rust", &authors[2]._id, &shelves[1]._id,
            &"Civil war", &authors[1]._id, &shelves[0]._id,
            &"The world we see", &authors[0]._id, &shelves[2]._id
        ]
    ).await?;

    println!("Books created successfully");

    let book_rows = client.query("SELECT id, title, author_id, shelf_id FROM book", &[]).await?;

    let books: Vec<Book> = book_rows.iter().map(|row| {
        Book{
            _id: row.get(0),
            title: row.get(1),
            author_id: row.get(2),
            shelf_id: row.get(3)
        }
    }).collect();

    for book in &books{
        println!("{:?}", book)
    }


    Ok(())
}
