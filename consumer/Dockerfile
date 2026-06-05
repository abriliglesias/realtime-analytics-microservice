
FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY internal/ ./internal/
COPY consumer/  ./consumer/

RUN CGO_ENABLED=0 GOOS=linux go build -o /consumer ./consumer

FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /consumer .

ENTRYPOINT ["./consumer"]
