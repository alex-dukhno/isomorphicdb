FROM rust:1.46 as build

COPY ./ ./

RUN cargo build --release

RUN mkdir -p /build-out

RUN cp target/release/isomorphicdb /build-out/

FROM ubuntu:20.04

RUN apt-get update \
    && apt-get install -y openssl

ENV RUST_LOG=debug
ENV ROOT_PATH=/var/lib/data
ENV PERSISTENT=1

EXPOSE 5432

COPY --from=build /build-out/isomorphicdb /

CMD /isomorphicdb
