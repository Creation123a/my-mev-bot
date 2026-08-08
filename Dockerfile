FROM ghcr.io/foundry-rs/foundry:latest

# Install Go and supervisor
RUN apt-get update && apt-get install -y golang-go supervisor && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build your bot (adjust if your main.go is in root)
RUN go mod download && go build -o /usr/local/bin/mevbot .

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY chaos.sh /usr/local/bin/chaos.sh
RUN chmod +x /usr/local/bin/chaos.sh

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
