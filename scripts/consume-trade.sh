#!/bin/bash

echo "📊 Running trade consumer..."

TOPIC=${2:-trades} # Default to `trades` if not specified

case "${1:-normal}" in
    "sequential")
        echo "Sequential processor test"
        go run cmd/tool/trade-consumer/main.go --processor sequential --verbose --topic $TOPIC
        ;;
    "normal")
        echo "Key-ordered processor: 10 workers"
        go run cmd/tool/trade-consumer/main.go --processor key-ordered --workers 10 --topic $TOPIC
        ;;
    "high")
        echo "High throughput: 50 workers"
        go run cmd/tool/trade-consumer/main.go --processor key-ordered --workers 50 --topic $TOPIC
        ;;
    "verbose")
        echo "Verbose logging with 10 workers"
        go run cmd/tool/trade-consumer/main.go --processor key-ordered --workers 10 --verbose --topic $TOPIC
        ;;
    *)
        echo "Usage: $0 [sequential|normal|high|verbose]"
        exit 1
        ;;
esac
