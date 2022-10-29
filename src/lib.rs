use reqwest::Response;
use serde::{Deserialize, Serialize};
use serde_json::{self, json};
use std::{vec::Vec, collections::HashMap};

#[derive(Deserialize, Serialize, Debug)]
pub struct HitsItem<T> {
    pub _index: String,
    pub _id: String,
    pub _type: String,
    pub _score: Option<f32>,
    pub _source: T,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Hits<T> {
    pub hits: Vec<HitsItem<T>>,
    pub max_score: Option<f32>,
    pub total: Total,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Shard {
    pub total: i32,
    pub successful: i32,
    pub skipped: i32,
    pub failed: i32,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Total {
    pub value: i32,
    pub relation: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SearchParams {
    pub size: Option<i32>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SearchResult<T> {
    pub took: i32,
    pub timed_out: bool,
    pub _shards: Shard,
    pub sort: Option<Vec<i32>>,
    pub hits: Hits<T>,
}

#[derive(Debug)]
pub struct ElasticsearchClient {
    client: reqwest::Client,
    pub address: String,
    pub port: i32,
}

impl ElasticsearchClient {
    pub fn new(addr: String, port: i32) -> Self {
        ElasticsearchClient {
            client: reqwest::Client::new(),
            address: addr,
            port: port,
        }
    }

    pub async fn get_index_names(
        &self,
        index_pattern: Option<&str>,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let url = format!("{}:{}/_cat/indices/{}?format=json", self.address, self.port, index_pattern.unwrap_or(""));
        let res = self.client
        .get(&url)
        .send()
        .await?
        .json::<Vec<HashMap<String, String>>>()
        .await?;

        Ok(res.iter().map(|doc| doc["index"].clone()).collect())
    }

    pub async fn search<T: for<'de> serde::Deserialize<'de> + Clone>(
        &self,
        index: &str,
        // search_param: Option<SearchParams>,
        search_param: Option<serde_json::Value>,
    ) -> Result<Vec<T>, Box<dyn std::error::Error>> {
        let url = format!("{}:{}/{}/_search", self.address, self.port, index);
        let res = match search_param {
            Some(param) => {
                self.client
                    .post(&url)
                    // .header("Content-Type", "application/json")
                    .json(&param)
                    .send()
                    .await?
                    .json::<SearchResult<T>>()
                    .await?
            }
            None => {
                self.client
                    .get(&url)
                    .send()
                    .await?
                    .json::<SearchResult<T>>()
                    .await?
            }
        };

        Ok(res
            .hits
            .hits
            .iter()
            .map(|doc| doc._source.clone())
            .collect())
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     #[test]
//     fn it_works() {
//         let result = main();
//         assert_eq!(result.is_ok(), true);
//     }
// }
