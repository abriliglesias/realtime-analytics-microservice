# Build ----------------
FROM golang:alpine AS builder
WORKDIR /app

# Copy the single go.mod/go.sum at the root first so Docker can cache
# the dependency download layer separately from the source code.
COPY go.mod go.sum ./
RUN go mod download

# Copy everything the producer needs:
#   - the shared internal/db package
#   - the producer source itself
COPY internal/ ./internal/
COPY producer/  ./producer/

# Build only the producer binary.
# The output path is irrelevant — it's copied to the final stage below.
RUN CGO_ENABLED=0 GOOS=linux go build -o /producer ./producer

# ── Run ──────────────────────────────────────────────────────────────
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /producer .

EXPOSE 8081
ENTRYPOINT ["./producer"]
