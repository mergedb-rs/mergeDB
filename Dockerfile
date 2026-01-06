# Builder
FROM rust:1.92.0-trixiec AS builder

WORKDIR /app

# Install protoc for tonic
RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

# Copy entire workspace
COPY . .

# Build only kv-node
RUN cargo build --release -p kv-node


# Runtime
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/kv-node /usr/local/bin/kv-node

EXPOSE 8000

CMD ["kv-node"]
