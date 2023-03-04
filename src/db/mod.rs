use crate::db::utilities::*;
use crate::{error::Error::*, handler::BookRequest, Book, Result};
use chrono::prelude::*;
use futures::StreamExt;
use mongodb::bson::{doc, document::Document, oid::ObjectId, Bson};
use mongodb::{options::ClientOptions, Client, Collection};

pub mod utilities;
#[derive(Clone, Debug)]
pub struct DB {
  pub client: Client,
}

impl DB {
  pub async fn init() -> Result<Self> {
    let mut client_options = ClientOptions::parse(MONGODB_URL).await?;
    client_options.app_name = Some(DB_NAME.to_string());

    Ok(Self {
      client: Client::with_options(client_options)?,
    })
  }

  pub async fn fetch_books(&self) -> Result<Vec<Book>> {
    let mut cursor = self
      .get_collection()
      .find(None, None)
      .await
      .map_err(MongoQueryError)?;

    let mut result = Vec::new();
    while let Some(doc) = cursor.next().await {
      result.push(self.doc_to_book(&doc?)?);
    }
    Ok(result)
  }

  pub async fn create_book(&self, entry: &BookRequest) -> Result<()> {
    let doc = doc! {
      NAME: entry.name.clone(),
      AUTHOR: entry.author.clone(),
      NUM_PAGES: entry.num_pages as i32,
      ADDED_AT: Utc::now(),
      TAGS: entry.tags.clone(),
    };
    self
      .get_collection()
      .insert_one(doc, None)
      .await
      .map_err(MongoQueryError)?;
    Ok(())
  }

  pub async fn edit_book(&self, id: &str, entry: &BookRequest) -> Result<()> {
    let obj_id = ObjectId::with_string(id).map_err(|_| InvalidIDError(id.to_owned()))?;
    let query = doc! {
      ID: obj_id
    };

    let doc = doc! {
      NAME: entry.name.clone(),
      AUTHOR: entry.author.clone(),
      NUM_PAGES: entry.num_pages as i32,
      ADDED_AT: Utc::now(),
      TAGS: entry.tags.clone(),
    };
    self
      .get_collection()
      .update_one(query, doc, None)
      .await
      .map_err(MongoQueryError)?;

    Ok(())
  }

  pub async fn delete_book(&self, id: &str) -> Result<()> {
    let obj_id = ObjectId::with_string(id).map_err(|_| InvalidIDError(id.to_owned()))?;
    let filter = doc! {
      ID: obj_id
    };
    self
      .get_collection()
      .delete_one(filter, None)
      .await
      .map_err(MongoQueryError)?;
    Ok(())
  }

  fn get_collection(&self) -> Collection {
    self.client.database(DB_NAME).collection(COLL)
  }

  fn doc_to_book(&self, doc: &Document) -> Result<Book> {
    let id = doc.get_object_id(ID)?;
    let name = doc.get_str(NAME)?;
    let author = doc.get_str(AUTHOR)?;
    let num_pages = doc.get_i32(NUM_PAGES)?;
    let added_at = doc.get_datetime(ADDED_AT)?;
    let tags = doc.get_array(TAGS)?;

    let book = Book {
      id: id.to_hex(),
      name: name.to_owned(),
      author: author.to_owned(),
      num_pages: num_pages as usize,
      added_at: *added_at,
      tags: tags
        .iter()
        .filter_map(|entry| match entry {
          Bson::String(v) => Some(v.to_owned()),
          _ => None,
        })
        .collect(),
    };
    Ok(book)
  }
}
