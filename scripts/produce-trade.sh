#!/bin/bash

echo "📈 Running trade producer..."

case "${1:-normal}" in
    "quick")
          echo "Quick test: 100 trades"
          go run cmd/tool/trade-producer/main.go --count 100 --rate 0
          ;;
    "normal")
          echo "Normal test: 1000 trades at 100/sec"
          go run cmd/tool/trade-producer/main.go --count 1000 --rate 100
          ;;
    "high")
          echo "High load: 5000 trades at 500/sec"
          go run cmd/tool/trade-producer/main.go --count 5000 --rate 500
          ;;
    "continuous")
          echo "Continuous: 5 minutes at 200/sec"
          go run cmd/tool/trade-producer/main.go --duration 5m --rate 200
          ;;
    *)
          echo "Usage: $0 [quick|normal|high|continuous]"
          exit 1
          ;;
esac
