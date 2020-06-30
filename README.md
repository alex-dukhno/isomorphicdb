# Database

![Rust](https://github.com/alex-dukhno/database/workflows/Rust/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/alex-dukhno/database/badge.svg)](https://coveralls.io/github/alex-dukhno/database)

The project doesn't have any name so let it be `database` for now.

## Project structure

 * `compatibility/` - folder contains `SQL` scripts to run functional tests through
                    the running system. It has name `compatibility` because it was intended
                    to emphasize compatibility testing with `PostgreSQL`, but now it is
                    more suite of `SQL` queries that database **should** support.
                    Strict testing must be automated and probably test suite will 
                    be written in another programming language (#65)
 * `proof-of-concept/` - there are a couple of sub projects/modules to play with concepts
                         before starting development of database. It could become
                         a play ground of different PoC.
 * `src/kernel/` - core concept of the system. All modules (except `protocol`) depends on it.
                   It should provide conceptual abstraction for other modules. Good examples
                   are `SystemResult` and `SystemError`. Other part of system uses them to
                   handle errors that are not local for a module but more system wide.
 * `src/node/` - database node (member, server, instance) code. Handles network communication
                 with clients and process management of incoming queries. It also contains
                 concrete `trait` implementations from `src/protocol/` module.
 * `src/protocol/` - server-side (backend) API of 
                    [PostgreSQL Wire Protocol](https://www.postgresql.org/docs/12/protocol.html)
                    The goal is to provide high level `trait`s and `struct`s to help other `rust`
                    database implementations be `PostgreSQL` compatible.
                    **Must** not depend on any modules of the system and has as small as possible
                    dependencies on other crates because it is intended to move out of the project
                    into completely separate crate. Right now it is in the project because of ease
                    of testing, prototyping and development.
 * `src/sql_engine/` - module to execute incoming `SQL` queries. That is it.
 * `src/sql_types/` - module contains code to support different `SQL` types and incoming data validation
 * `src/storage/` - module to persist and retrieve execution results of `SQL` queries. It is split
                    into two major parts: `FrontendStorage` and `BackendStorage`.
                    `FrontendStorage` is responsible to provide high level `relational` API like `CREATE SCHEMA`,
                    `CREAT TABLE` and `CREATE INDEX`. `BackendStorage` is responsible to hide actual on-disk
                    storage implementation and provide low level API for `FrontendStorage` like create a namespace,
                    create an object, read/write data in binary format from/to disk.

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
1. After that you can run tests with:
    ```shell script
    pytest -v tests/functional/generic_tests.py
    ```

`pip3` and `python3` OR `pip` and `python` - depends on your system and your 
preferences.

For system with both `python` 2 and 3 - use `python3` and `pip3` to run tests 
with the 3rd version of `python`.