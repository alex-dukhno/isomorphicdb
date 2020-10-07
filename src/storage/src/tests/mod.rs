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

use super::*;
use binary::{Binary, Row};

#[cfg(test)]
mod in_memory;
#[cfg(test)]
mod persistent;

#[rstest::fixture]
fn schema_name() -> SchemaName<'static> {
    "schema_name"
}

#[rstest::fixture]
fn object_name() -> ObjectName<'static> {
    "object_name"
}

fn as_rows(items: Vec<(u8, Vec<&'static str>)>) -> Vec<Row> {
    items
        .into_iter()
        .map(|(key, values)| {
            let k = Binary::with_data(key.to_be_bytes().to_vec());
            let v = Binary::with_data(
                values
                    .into_iter()
                    .map(|s| s.as_bytes())
                    .collect::<Vec<&[u8]>>()
                    .join(&b'|'),
            );
            (k, v)
        })
        .collect()
}

fn as_keys(items: Vec<u8>) -> Vec<Key> {
    items
        .into_iter()
        .map(|key| Binary::with_data(key.to_be_bytes().to_vec()))
        .collect()
}

fn as_read_cursor(items: Vec<(u8, Vec<&'static str>)>) -> ReadCursor {
    Box::new(items.into_iter().map(|(key, values)| {
        let k = key.to_be_bytes().to_vec();
        let v = values
            .into_iter()
            .map(|s| s.as_bytes())
            .collect::<Vec<&[u8]>>()
            .join(&b'|');
        Ok(Ok((Binary::with_data(k), Binary::with_data(v))))
    }))
}
