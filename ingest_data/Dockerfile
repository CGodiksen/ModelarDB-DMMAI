FROM rust:latest

WORKDIR /usr/src/app

COPY . .

RUN cargo build --profile dev

CMD ["target/debug/ingest_data"]
