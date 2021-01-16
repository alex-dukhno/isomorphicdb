# Welcome to the Project 🎉

First off, thank you for interest in the project.

* You want to ask a question?
* You have an idea?
* You want to give a feedback?
* or You want to say "Hi"?

Then we are more than welcome to see you on our [discord server](https://discord.gg/PUcTcfU). 
It is our main point of discussing issues, ideas and current development areas.

### Contributing the code

Don't know where to start to contribute the code?
You can:
 1. peek up issue with [help wanted](https://github.com/alex-dukhno/isomorphicdb/labels/help%20wanted)
or [good first issue](https://github.com/alex-dukhno/isomorphicdb/labels/good%20first%20issue)
labels.
 1. submit an issue with proposal to work on interested area for you
 1. ask question on discord server which issue to pick up
 1. have a code changes but stuck with rust/project structure submit a `draft` PR and ask for feedback

PR, to be merged, has to pass `formatting`, `clippy` and `unit tests` checks.
To help you pass CI we have `clippy.sh` and `rustfmt.sh` in `local` folder.
All you need is to run following commands and see that there are no errors:
```shell script
./local/rustfmt.sh
./local/clippy.sh
cargo test
```

One of the sub-goal of the project is to develop general 
[PostgreSQL Wire Protocol](https://www.postgresql.org/docs/current/protocol.html)
backend in rust, so that rust and database communities could benefit from. So I would
love external help on it.
