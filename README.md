# IsomorphicDB

![Merge](https://github.com/alex-dukhno/isomorphicdb/workflows/Merge/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/alex-dukhno/isomorphicdb/badge.svg?branch=main)](https://coveralls.io/github/alex-dukhno/isomorphicdb?branch=main)
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
* [ ] High ingestion rate with support Kafka compatible persistent queues as an external WAL

## Play around with project

See [docs](./docs/.)

## Project structure

* `ci/` - script helpers to run commands on `GitHub Actions`
* `data/` - group of modules responsible for manipulating data, database structure
    * `data/binary/` - representing primitive types as a raw binary vector
    * `data/catalog/` - API for accessing data and its definition
    * `data/schema_planner` - module that transform `Data Definition Language` queries
    analysis into `data_definition/operations`
* `data_definition/` - group of modules responsible to represent `SQL DDL` queries
    * `data_definition/operations` - data structures responsible for representing
      operations of `Data Definition Language` part of `SQL`
* `docs/` - project documentation
* `gradle/` - gradle wrapper to run `tests/compatibility` tests
* `local/` - helper scripts to fix `rustfmt` and `clippy` errors
* `query_analysis/` - modules responsible for *Analysis* phase in query execution pipeline
    * `query_analysis/expr_operators/` - expression operators of analysis tree
    * `query_analysis/query_analyzer/` - API to analyse parsed SQL query
    * `query_analysis/tree/` - structured representation of analyzed SQL query
* `query_parsing/` - modules responsible for *Parsing* phase in query execution pipeline
    * `query_parsing/parser/` - query parser
    * `query_parsing/sql-ast/` - structured representation of parsed SQL query
* `repr/` - building blocks to how transform binary representation into values
* `server/` - database server
    * `server/node/` - entry point that starts database application
    * `server/connection/` - mechanics to establish connection between clients and server
    * `server/pg_model/` - data structure that is used to transform database query results into PG representation
* `tests/compatibility` - groovy based tests to check compatibility with [PostgreSQL](https://www.postgresql.org/)
* `tests/erlang_client` - erlang based tests
* `tests/fixtures` - files needed to set up non-default local testing
* `types/` - SQL types

## Development

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

1. Install `grcov`
1. Run `./local/code_coverage.sh`
1. Open `./target/debug/coverage/index.html` in your browser

### Running Compatibility tests locally

1. Install `java` version `8` or `11`(that were tested)
1. (Optional) Install `gradle` version `6` (that were tested)
1. Run `PERSISTENT=1 RUST_LOG=debug cargo run` from project folder in separate terminal window
1. Run `./local/compatibility.sh`

### Running Erlang Client Compatibility tests locally

1. Install `Erlang` version `23.1` (which is tested on CI now). You could
install specified version of Erlang via [asdf](https://github.com/asdf-vm/asdf).
1. Install [rebar3](https://github.com/erlang/rebar3) to run Erlang Common Test.
1. Run `./ci/erlang_client.sh`.
1. Kill `isomorphicdb` process manually for running the tests again.

### Running Functional tests

We use PyTest for functional tests. To run tests locally you need to set up
`python` environment with the following commands:
1. If you use linux or macos it is most probably you have `python` installed.
For windows, you can easily install `python` from [official site](https://www.python.org).
1. Install `python` dependencies with `pip` requirements executing the following command:
    ```shell script
    pip install -r tests/functional/requirements.txt
    ```
1. or with `pip3`:
    ```shell script
    pip3 install -r tests/functional/requirements.txt
    ```
1. After that you can run all tests with:
   ```shell script
   pytest -v tests/functional/
   ```
1. or tests in specified file with:
    ```shell script
    pytest -v tests/functional/<test_file>.py
    ```

`pip3` with `python3` OR `pip` with `python` - depends on your system and your 
preferences.

For system with both `python` 2 and 3 - use `python3` and `pip3` to run tests
with the 3rd version of `python`.
