# Stage 1: Builder
FROM rust:1.85-slim-bookworm AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./

# Create dummy src to build dependencies (faster rebuilds)
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release && rm -rf src

# Copy actual source and rebuild
COPY src ./src
RUN touch src/main.rs && cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false appuser

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/target/release/standx-orderbook .

# Create data directory for wallet_history.csv
RUN mkdir -p /app/data && chown -R appuser:appuser /app

USER appuser

# Default command
CMD ["./standx-orderbook"]
