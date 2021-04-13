# IsomorphicDB

![Merge](https://github.com/alex-dukhno/isomorphicdb/workflows/Merge/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/alex-dukhno/isomorphicdb/badge.svg?branch=main)](https://coveralls.io/github/alex-dukhno/isomorphicdb?branch=main)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Falex-dukhno%2Fisomorphicdb.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Falex-dukhno%2Fisomorphicdb?ref=badge_shield)
<a href="https://discord.gg/PUcTcfU"><img src="https://img.shields.io/discord/509773073294295082.svg?logo=discord"></a>

## TODO List

* [ ] PostgreSQL compatibility
    * [ ] PostgreSQL wire protocol
    * [ ] Data types
    * [ ] Data definition language
        * [ ] Create/Drop/Alter table
        * [ ] Create/Drop index
        * [ ] Primary/Foreign keys
        * [ ] Check constraints
    * [ ] Data manipulation language
* [ ] Transactions
* [ ] Reactive Dataflow query execution
    * [ ] With RSocket for inter node communication
* [ ] Query compilation
* [ ] HTAP (Hybrid transactional/analytical processing)
* [ ] Raft replication
* [ ] Operability
    * [ ] Smooth version upgrade
    * [ ] Ease of adding/removing node from the cluster (including quick replication)
    * [ ] Self-driving

## Play around with project

See [docs](./docs/.)

## Project structure

* `docs/` - project documentation
* `etc/` - scripts for convenient run of compatibility tests and other tools
    * `ci/` - script helpers to run commands on `GitHub Actions`
    * `local/` - scripts for local usage
* `gradle/` - gradle wrapper to run `tests/compatibility` tests
* `node_engine/` - module that glues all other together to handle incoming network request and execute it across other modules
* `postgres/` - crate to consolidate PostgreSQL functionality
    * `query_ast/` - abstract syntax tree of parsed SQL query
    * `query_parser/` - parser that produce AST from SQL string
    * `query_response/` - module to represent successful or error response after query execution to a client
* `sql_engine/` - crate to consolidate SQL query engine functionality
    * `catalog/` - API for accessing data and its definition
    * `data_definition/` - group of modules responsible to represent `SQL DDL` queries
        * `execution_plan` - data structures responsible for representing operations of `Data Definition Language` part of `SQL`
    * `data_manipulation/` - group of modules responsible to represent `SQL DML` queries
        * `operators/` - SQL operators like `+`, `-`, `LIKE`, `AND`, `OR` and others
        * `query_plan/` - query plan that is executed over database data
        * `query_result/` - internal representation of query execution result
        * `typed_queries/` - represents query structure that is ready for type coercion and type check
        * `typed_tree/` - typed binary tree of SQL operators
        * `untyped_queries/` - represents query structure that is ready for type resolution
        * `untyped_tree/` - untyped binary tree of SQL operators
    * `definition_planner/` - API to create execution plan for a DDL query
    * `entities/` - database entities
        * `definition/` - database object names and its definitions
        * `types/` - SQL types
    * `query_analyzer/` - API to analyse a parsed SQL query
    * `query_planner/` - API to create execution plan for a SQL query
    * `query_processing/` - API to process a parsed/analyzed SQL query
        * `type_check/`
        * `type_coercion/`
        * `type_inference/`
    * `scalar/` - representing primitive types as a scalar value that can be use as intermediate computational result
* `storage/` - database transactional storage
    * `api/` - type aliases and traits that defines api for `in_memory` and `persistent` storage
    * `binary/` - representing primitive types as a raw binary vector
    * `in_memory/` - in memory only storage
    * `persistent/` - persistent storage
* `tests/`
    * `compatibility/` - groovy based tests to check compatibility with [PostgreSQL](https://www.postgresql.org/)
    * `erlang_client/` - erlang based tests
    * `fixtures/` - files needed to set up non-default local testing

## Development

### Build time dependencies

`isomorphicdb` uses [postgres-parser](https://github.com/zombodb/postgres-parser) to parse PostgreSQL 13 SQL syntax which
requires LLVM. Thus, to build project you need to install LLVM and add it to `$PATH`

On Ubuntu the following command should be sufficient:
```shell
sudo apt install llvm
```

On MacOS with `zsh`
```shell
brew install llvm
echo 'export PATH="/usr/local/opt/llvm/bin:$PATH"' ~/.zshrc
source ~/.zshrc 
```

### Setting up integration suite for local testing

For now, it is local and manual - that means some software has to be installed 
on a local machine and queries result has to be checked visually.

1. Install `psql` ([PostgreSQL client](https://www.postgresql.org))
    1. Often it comes with `PostgreSQL` binaries. On macOS, you can install it 
    with `brew` executing the following command:
    ```shell script
    brew install postgresql
    ```
1. Start the `isomorphicdb` instance with the command:
    ```shell script
    cargo run
    ```
1. Start `psql` with the following command:
    ```shell script
    psql -h 127.0.0.1 -W
    ```
    1. enter any password
1. Run `sql` scripts from `compatibility` folder

### Compute code coverage locally

1. Run `./etc/local/code_coverage.sh`
1. Open `./target/debug/coverage/index.html` in your browser

### Running Compatibility tests locally

1. Install `java` version `8` or `11`(that were tested)
1. (Optional) Install `gradle` version `6` (that were tested)
1. Run `RUST_LOG=debug cargo run` from project folder in separate terminal window
1. Run `./etc/local/compatibility.sh`

### Running Erlang Client Compatibility tests locally

1. Install `Erlang` version `23.1` (which is tested on CI now). You could
install a specified version of Erlang via [asdf](https://github.com/asdf-vm/asdf).
1. Install [rebar3](https://github.com/erlang/rebar3) to run Erlang Common Test.
1. Run `./etc/ci/erlang_client.sh`.
1. Kill `isomorphicdb` process manually for running the tests again.



## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Falex-dukhno%2Fisomorphicdb.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Falex-dukhno%2Fisomorphicdb?ref=badge_large)