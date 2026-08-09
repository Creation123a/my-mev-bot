# 1. Use the official lightweight Go environment
FROM golang:1.22-alpine AS builder

# 1. Install build essentials for submodules
RUN apk add --no-cache git gcc musl-dev

WORKDIR /app

# 2. Copy code into the container
COPY . .

# 3. Synchronize cross-module dependencies and force-fix missing hashes
RUN go mod tidy

# 4. Compile directly from the root folder
RUN go build -o bot-binary .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/bot-binary .

CMD ["./bot-binary"]
