# 1. Use the official lightweight Go environment
FROM golang:1.22-alpine AS builder
WORKDIR /app

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# This fixed line builds your main file automatically without needing a specific filename!
RUN go build -o bot-binary .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/bot-binary .

CMD ["./bot-binary"]
