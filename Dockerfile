FROM rust:1.94.0-alpine3.23 AS builder

WORKDIR /app

# Copy manifest and source files
COPY . .

# Build the application
RUN cargo build --release

FROM alpine:3.23 AS runtime

RUN apk --no-cache upgrade

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/nox-ingestor-replayer .

# Run the application
ENTRYPOINT ["/app/nox-ingestor-replayer"]
