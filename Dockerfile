# 1. Use the official lightweight Go environment
FROM golang:1.22-alpine AS builder

# Install build dependencies (needed for compiling certain Go network modules)
RUN apk add --no-cache git gcc musl-dev

WORKDIR /app

# Copy all repository code into the container
COPY . .

# Force Go to automatically fetch all missing modules and synchronize internal packages
RUN go mod tidy

# Compile the package cleanly into the execution binary
RUN go build -o bot-binary .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/bot-binary .

CMD ["./bot-binary"]
