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

use fail::FailScenario;
use storage::{ObjectId, SchemaId};

#[rstest::fixture]
pub fn scenario() -> FailScenario<'static> {
    FailScenario::setup()
}

pub const SCHEMA: SchemaId = "schema_name";
#[allow(dead_code)]
pub const OBJECT: ObjectId = "object_name";
