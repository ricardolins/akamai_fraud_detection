#!/usr/bin/env bash
# Downloads the large AWS SDK bundle JAR that is excluded from git (>100MB).
# Run once before building the spark-jobs image.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"

download() {
  local jar="$1" url="$2" dest="${DIR}/${1}"
  if [ -f "$dest" ]; then
    echo "${jar} already present, skipping download."
    return 0
  fi
  echo "Downloading ${jar} ..."
  curl -fL --progress-bar -o "$dest" "$url"
  echo "Done: $dest"
}

download "aws-java-sdk-bundle-1.12.262.jar" \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"

# Needed for org.apache.iceberg.aws.s3.S3FileIO (the Nessie/Iceberg catalog's
# write path) — it depends on the AWS SDK v2 classes, which aws-java-sdk-bundle
# (v1, used by hadoop-aws's S3AFileSystem for the s3a:// bronze read path)
# does not provide.
download "iceberg-aws-bundle-1.5.2.jar" \
  "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar"
