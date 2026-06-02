# Build ----------------
FROM golang:1.22-alpine AS builder

# Install necessary dependencies for compilation if needed
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Copy module definition files first (to leverage Docker cache)
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Compile the optimized binary for container environments:
# - CGO_ENABLED=0: Disables dynamic linking to C libraries, generating a purely static binary.
# - GOOS=linux: Ensures the binary is compiled for the Linux kernel.
# - ldflags "-s -w": Reduces binary size by removing the symbol table and debugging information.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o analytics-producer ./main.go

# Minimal Execution Environment ----------------
FROM alpine:3.19 AS runner

# Create a non-privileged user for security reasons (Principle of least privilege)
RUN adduser -D -g '' appuser

# Copy certificates and timezone data from the build stage
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

WORKDIR /app

# Copy only the compiled binary from the previous stage
COPY --from=builder /app/analytics-producer .

# Change file ownership to the non-root user
RUN chown -R appuser:appuser /app

# Switch to the non-privileged user
USER appuser

# Expose the port where the REST API listens
EXPOSE 8080

# Default command to start the microservice
CMD ["./analytics-producer"]