#!/bin/bash

CLEAN_VOLUMES=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --clean|-c)
            CLEAN_VOLUMES=true
            shift
            ;;
        *)
            echo "Usage: $0 [--clean|-c]"
            echo "  --clean, -c    Also remove volumes (data will be lost)"
            exit 1
            ;;
    esac
done

echo "🛑 Stopping all services..."

if [ "$CLEAN_VOLUMES" = true ]; then
    echo "⚠️  Also removing volumes (data will be lost)..."
    docker compose down -v
    echo "🗑️  Volumes removed"
else
    docker compose down
    echo "💾 Data volumes preserved"
fi

echo ""
echo "✅ All services stopped"