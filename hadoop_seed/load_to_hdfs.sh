#!/usr/bin/env bash
# Push locally generated ingest_date=YYYY-MM-DD partitions into the Docker HDFS NameNode.
# Each partition is replaced as a whole, so re-running for the same date is idempotent.
#
# Usage: hadoop_seed/load_to_hdfs.sh [SEED_DIR]   (default: seed_data)
set -euo pipefail

SEED_DIR="${1:-seed_data}"
HDFS_TARGET="${HDFS_SOURCE_PATH:-/legacy/erp/orders}"
CONTAINER="${NAMENODE_CONTAINER:-namenode}"
STAGING="/tmp/seed_upload"

if [[ ! -d "$SEED_DIR" ]]; then
  echo "Seed directory '$SEED_DIR' not found; run 'make seed' first." >&2
  exit 1
fi

shopt -s nullglob
partitions=("$SEED_DIR"/ingest_date=*)
if [[ ${#partitions[@]} -eq 0 ]]; then
  echo "No ingest_date=* partitions in '$SEED_DIR'." >&2
  exit 1
fi

if ! docker exec "$CONTAINER" true 2>/dev/null; then
  echo "Container '$CONTAINER' is not running; run 'make up' first." >&2
  exit 1
fi

echo "Waiting for HDFS to leave safe mode..."
docker exec "$CONTAINER" hdfs dfsadmin -safemode wait

for partition in "${partitions[@]}"; do
  name="$(basename "$partition")"
  echo "Loading $name -> $HDFS_TARGET/$name"
  docker exec "$CONTAINER" rm -rf "$STAGING/$name"
  docker exec "$CONTAINER" mkdir -p "$STAGING"
  docker cp "$partition" "$CONTAINER:$STAGING/$name"
  docker exec "$CONTAINER" bash -c "
    set -e
    hdfs dfs -rm -r -f -skipTrash '$HDFS_TARGET/$name' >/dev/null
    hdfs dfs -mkdir -p '$HDFS_TARGET/$name'
    hdfs dfs -put -f '$STAGING/$name'/*.csv '$HDFS_TARGET/$name/'
    rm -rf '$STAGING/$name'
  "
done

docker exec "$CONTAINER" hdfs dfs -ls -R "$HDFS_TARGET"
