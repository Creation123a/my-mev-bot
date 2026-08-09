# 1. Use the official lightweight Go environment
FROM golang:1.21-alpine AS builder

# 2. Set the internal working path
WORKDIR /app

# 3. Copy your package files first to cache dependencies
COPY go.mod go.sum* ./
RUN go mod download

# 4. Copy the rest of your repository code
COPY . .

# 5. Force the compilation directly on your target dry-run file
# CHANGE "your_dry_run_file.go" to the exact name of your root dry-run file
RUN go build -o bot-binary your_dry_run_file.go

# 6. Final lightweight execution container stage
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/bot-binary .

# 7. Start the executable
CMD ["./bot-binary"]
