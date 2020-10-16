# Database

![Merge](https://github.com/alex-dukhno/database/workflows/Merge/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/alex-dukhno/database/badge.svg?branch=master)](https://coveralls.io/github/alex-dukhno/database?branch=master)
<a href="https://discord.gg/PUcTcfU"><img src="https://img.shields.io/discord/509773073294295082.svg?logo=discord"></a>

The project doesn't have any name so let it be `database` for now.

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

 * `docs/` - project documentation
 * `local/` - helper scripts to fix `rustfmt` and `clippy` errors
 * `src/ast/` - abstract syntax tree transformed from `sqlparser::ast` that can be evaluated by database engine
 * `src/binary/` - binary representation of data to store/read it on/from disk
 * `src/binder/` - module responsible for executing `Bind` message from client from extended query PostgreSQL protocol
 * `src/constraints/` - mechanics to ensure data validity that are going to store
 * `src/data_manager/` - module that manages how to, where store/read data and metadata
 * `src/description/` - building blocks of statement description, the info per statement is used to send statement description
                        on client request of `Describe` message
 * `src/expr_eval/` - module that evaluates static and dynamic expression in transformed `ast`
 * `src/kernel/` - core concept of the system. All modules (except `protocol`) depends on it.
                   It should provide conceptual abstraction for other modules. Good examples
                   are `SystemResult` and `SystemError`. Other part of system uses them to
                   handle errors that are not local for a module but more system wide.
 * `src/meta_def/` - database structure definitions (table, columns etc)
 * `src/metada/` - module responsible for managing on disk data about data
 * `src/node/` - database node (member, server, instance) code. Handles network communication
                 with clients and process management of incoming queries. It also contains
                 concrete `trait` implementations from `src/protocol/` module.
 * `src/parser/` - wrapper around `sqlparaser-rs` crate
 * `src/plan/` - building blocks for execution query plans
 * `src/protocol/` - server-side (backend) API of 
                    [PostgreSQL Wire Protocol](https://www.postgresql.org/docs/12/protocol.html)
                    The goal is to provide high level `trait`s and `struct`s to help other `rust`
                    database implementations be `PostgreSQL` compatible.
                    **Must** not depend on any modules of the system and has as small as possible
                    dependencies on other crates because it is intended to move out of the project
                    into completely separate crate. Right now it is in the project because of ease
                    of testing, prototyping and development.
 * `src/query_analyzer/` - module responsible for query analysis/description
 * `src/query_exeuctor/` - module to execute incoming `SQL` queries. That is it.
 * `src/query_planner/` - module uses `sqlparser::ast` to generate a plan how to execute query
 * `src/repr/` - building blocks to how transform binary representation into values
 * `src/sql_model/` - module contains code to support different `SQL` types other `SQL`
 * `src/storage/` - module abstracts work with `schema`s, `table`s and other on disk structures
 * `tests/compatibility` - groovy based tests to check compatibility with [PostgreSQL](https://www.postgresql.org/)
 * `tests/fixtures` - files needed to set up non-default local testing
 * `tests/functional` - python based tests to check database functionality (eventually should be merged with compatibility tests)

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
1. Start the `database` instance with the command:
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
