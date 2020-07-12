// Copyright 2020 Alex Dukhno
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::query_listener::SmolQueryListener;
use protocol::{listener::ProtocolConfiguration, QueryListener};
use std::env;

const PORT: usize = 5432;
const HOST: &str = "0.0.0.0";

pub fn start() {
    let local_address = format!("{}:{}", HOST, PORT);
    log::debug!("Starting server on {}", local_address);

    smol::run(async {
        let secure = match env::var("SECURE") {
            Ok(s) => match s.to_lowercase().as_str() {
                "ssl_only" => ProtocolConfiguration::ssl_only(),
                "gssenc_only" => ProtocolConfiguration::gssenc_only(),
                "both" => ProtocolConfiguration::both(),
                _ => ProtocolConfiguration::none(),
            },
            _ => ProtocolConfiguration::none(),
        };

        let listener = SmolQueryListener::bind(local_address, secure)
            .await
            .expect("open server connection");

        log::debug!("start server");
        listener.start().await.unwrap().unwrap();
    });
}
