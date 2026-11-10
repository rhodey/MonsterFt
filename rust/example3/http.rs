use monsterft::{
    Value,
    error::{Error, Result},
};
use serde::Deserialize;
use serde_json::{Value as Json, json};

#[derive(Deserialize)]
#[serde(tag = "method")]
enum Command {
    #[serde(rename = "POST")]
    Post {
        #[serde(rename = "requestId")]
        request_id: String,
        fact: Json,
    },
    #[serde(rename = "GET")]
    Get {
        #[serde(rename = "requestId")]
        request_id: String,
        #[serde(rename = "observationId")]
        observation_id: String,
    },
}

pub async fn execute(
    client: &reqwest::Client,
    url: &str,
    node: &str,
    command: Option<&[u8]>,
) -> Result<Value> {
    // Database setup has no external side effect.
    let Some(command) = command else {
        return Ok(Value::Nil);
    };

    let command = serde_json::from_slice(command).map_err(|e| Error::application(e.to_string()))?;

    let request = match command {
        Command::Post { request_id, fact } => {
            let body = json!({
                "requestId": request_id,
                "fact": fact,
            });

            client
                .post(url)
                .header("MonsterFt-Node-Id", node)
                .json(&body)
        }
        Command::Get {
            request_id,
            observation_id,
        } => {
            let query = [("requestId", request_id), ("observationId", observation_id)];

            client.get(url).query(&query)
        }
    };

    let response = request
        .send()
        .await
        .map_err(|e| Error::application(e.to_string()))?;

    let status = response.status();
    let value: Json = response
        .json()
        .await
        .map_err(|e| Error::application(e.to_string()))?;

    if !status.is_success() {
        let message = value["error"].as_str().unwrap_or("HTTP request failed");

        return Err(Error::application(message));
    }

    // Return the service result as the application value compared by MonsterFt.
    rmpv::ext::to_value(value).map_err(|e| Error::application(e.to_string()))
}
