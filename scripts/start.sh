#!/bin/bash
echo "🐼 Starting Redpanda environment..."

docker compose up -d
echo "⏳ Waiting for Redpanda to be ready..."
sleep 10

echo "🌐 Services available at:"
echo "   Redpanda Console: http://localhost:8080"
echo "   Kafka API: localhost:19092"
