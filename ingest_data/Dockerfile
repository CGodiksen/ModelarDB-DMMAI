FROM rust:latest

WORKDIR /usr/src/app

COPY . .

RUN cargo build --profile release

CMD ["target/release/ingest_data"]
