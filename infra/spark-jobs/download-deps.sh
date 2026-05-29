#!/usr/bin/env bash
# Downloads the large AWS SDK bundle JAR that is excluded from git (>100MB).
# Run once before building the spark-jobs image.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"

JAR="aws-java-sdk-bundle-1.12.262.jar"
URL="https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/${JAR}"
DEST="${DIR}/${JAR}"

if [ -f "$DEST" ]; then
  echo "${JAR} already present, skipping download."
  exit 0
fi

echo "Downloading ${JAR} ..."
curl -fL --progress-bar -o "$DEST" "$URL"
echo "Done: $DEST"
