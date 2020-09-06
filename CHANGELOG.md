# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project aims to adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
Check https://github.com/alex-dukhno/database/commits/master for undocumented changes.

### Changed

### Added
 - dynamic expression evaluation in `UPDATE` queries (#258) by @AndrewBregger

### Fixed

## [0.1.3] - 2020-07-22

### Added
 - database can run in a persistent mode, your data will survive a restart (#247) 
 - supported `DROP SCHEMA ... CASCADE` (#253)
 - docker container runs in persistent mode by default (#257)

## [0.1.2] - 2020-07-22

### Added
- support of simple query for PostgreSQL wire protocol
- support of simplest `insert`, `update`, `delete` and `select` queries
- handling errors in column names for `update` and `select` queries (#74, #100) by @silathdiir
- start publishing `latest` docker image using GitHub package
- added a documentation how to setup dockerized database (#131)
- support for SSL connection with a client (#169) by @silathdiir
- developer hint when type constraint validation fails (#174) by @Tolledo
- support of `serial` (`smallserial`, `serial` and `bigserial`) SQL types (#135) by @suhaibaffan
- support of `boolean` SQL type (#143) by @lpiepiora
- validation when user inserts more values than a table has columns (#137) by @AndrewBregger
- handling insert queries with specified column names (#124) by @silathdiir
- support of `char(n)` and `varchar(n)` SQL types (#97)
- support of `integer` (`smallint`, `integer` and `bigint`) SQL types (#95)
- functional tests migrated to run with python (#117) by @Aleks0010V
 