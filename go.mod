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
    my-mev-bot/config => ./Bot/Config
    my-mev-bot/dashboard => ./Bot/Dashboard
    my-mev-bot/execution => ./Bot/Execution
    my-mev-bot/ingestion => ./Bot/Ingestion
    my-mev-bot/solver => ./Bot/Solver
    my-mev-bot/state => ./Bot/State
    my-mev-bot/types => ./Bot/Types
)
