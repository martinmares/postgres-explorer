#!/usr/bin/env bash
# Build a run script pro postgres-explorer Docker image

set -euo pipefail

IMAGE_NAME="postgres-explorer"
IMAGE_TAG="latest"

echo "🔨 Building Docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .

echo ""
echo "✅ Image úspěšně vytvořen!"
echo ""
echo "📦 Velikost image:"
docker images "${IMAGE_NAME}:${IMAGE_TAG}" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"

echo ""
echo "🚀 Spuštění kontejneru:"
echo "   docker run -it --rm -p 8080:8080 ${IMAGE_NAME}:${IMAGE_TAG}"
echo ""
echo "🔍 S vlastní DB connection string:"
echo "   docker run -it --rm -p 8080:8080 -e DATABASE_URL='postgres://user:pass@host/db' ${IMAGE_NAME}:${IMAGE_TAG}"
echo ""
echo "🐚 Interaktivní shell v kontejneru:"
echo "   docker run -it --rm ${IMAGE_NAME}:${IMAGE_TAG} /bin/bash"
