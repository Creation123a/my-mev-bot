module my-mev-bot

go 1.22

require (
    github.com/andybalholm/brotli v1.1.0
    github.com/ethereum/go-ethereum v1.13.15
    github.com/gorilla/websocket v1.5.1
    github.com/joho/godotenv v1.5.1
    golang.org/x/sys v0.18.0
)

// Replace directives map the canonical import paths to the local subdirectories.
// This allows imports like "my-mev-bot/config" to resolve to ./config.
// After moving files into the appropriate directories and running `go mod tidy`,
// the project will build correctly.
replace (
    my-mev-bot/config => ./config
    my-mev-bot/dashboard => ./dashboard
    my-mev-bot/execution => ./execution
    my-mev-bot/ingestion => ./ingestion
    my-mev-bot/solver => ./solver
    my-mev-bot/state => ./state
    my-mev-bot/types => ./types
)