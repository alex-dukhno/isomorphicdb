# Getting Started

Currently, `database` is not distributed in a binary format. Experimentally, we
support docker image. So you have to have Docker installed on your machine. 
Before pooling it from `GitHub` registry you have to perform the following steps:

1. Generate and save `GitHub` token.
    1. Go to `Settings` -> `Developer Settings` -> `Personal access tokens`
    1. Click `Generate new token`
    1. Choose `read:packages` scope (you can choose others but this one is required)
    1. Click `Generate token`
1. It will be shown only once so save it to the file on your machine
1. Run the following command to login to `GitHub` docker registry
```shell script
cat /path/to/file/with/token | docker login https://docker.pkg.github.com -u <your github username> --password-stdin
```

After that you can pool image with the following command:
```shell script
docker pull docker.pkg.github.com/alex-dukhno/database/database:latest
```

To start up application you need to invoke the following command:
```shell script
docker run -it -d -p 5432:5432 docker.pkg.github.com/alex-dukhno/database/database
``` 

If you crashed the database docker instance we highly appreatiate if you rerun
scenario with:
```shell script
docker run -it -d -e RUST_BACKTRACE=1 -p 5432:5432 docker.pkg.github.com/alex-dukhno/database/database
```
and post a bug with backtrace. Thanks.

To connect to database you have to have `psql` on your machine, it can be installed
with `PostgreSQL` from the [official website](https://www.postgresql.org) or with
package manager like `homebrew` or `apt-get`.

Then you can start client with the command:
```shell script
psql -h 127.0.0.1 -W
```

After entering random password you should see `psql` prompt similar to:
```
psql (12.2, server 0.0.0)
Type "help" for help.

alex-dukhno=>
``` 

Now you can run `SQL` queries. Those that are currently supported by the database
you can find in `sql/` folder. Files are self contained, meaning that if you run
queries one-by-one they will create all needed schemas and tables to invoke 
`insert`s, `select`s, `update`s and `delete`s.
