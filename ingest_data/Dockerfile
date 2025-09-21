FROM rust:latest

WORKDIR /usr/src/app

COPY . .

RUN apt-get update && apt-get install -y protobuf-compiler


RUN cargo build --profile dev

CMD ["target/debug/ingest_data"]
