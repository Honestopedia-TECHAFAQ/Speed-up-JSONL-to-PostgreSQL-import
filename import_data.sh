#!/bin/bash
set -e

RATEHAWK_COMPRESSED_DUMP_URL="https://partner-feedora.s3.eu-central-1.amazonaws.com/feed/preferable_inventory_feed_en_v3.jsonl.zst"
TEMP_DIR=$(mktemp -d)
trap '[[ -d "$TEMP_DIR" ]] && rm -rf "$TEMP_DIR" && echo "Temporary directory removed."' EXIT

RATEHAWK_DUMP_COMPRESSED="$TEMP_DIR/ratehawk-dump.json.zst"
RATEHAWK_DUMP="$TEMP_DIR/ratehawk-dump.json"
RATEHAWK_HOTELS_CSV="$TEMP_DIR/ratehawk-hotels.csv"
RATEHAWK_ROOMS_CSV="$TEMP_DIR/ratehawk-rooms.csv"

DB_USER="postgres"
DB_NAME="postgres"
PSQL_EXEC="psql -U $DB_USER -d $DB_NAME"

# Faster Download using aria2c (if available)
if command -v aria2c &> /dev/null; then
    aria2c -x 16 -s 16 -o "$RATEHAWK_DUMP_COMPRESSED" "$RATEHAWK_COMPRESSED_DUMP_URL"
else
    curl -o "$RATEHAWK_DUMP_COMPRESSED" "$RATEHAWK_COMPRESSED_DUMP_URL"
fi

# Faster Decompression using multi-threading
zstd -T0 -d -o "$RATEHAWK_DUMP" "$RATEHAWK_DUMP_COMPRESSED"

# Convert JSONL to CSV in Parallel
export RATEHAWK_HOTELS_CSV RATEHAWK_ROOMS_CSV
jq -c '. | {hotels: ., rooms: .room_groups}' "$RATEHAWK_DUMP" | \
    parallel --pipe --block 10M "jq -r ' 
    select(.hotels) | [
        .hotels.id, .hotels.name, (.hotels.images | join(",")), .hotels.phone, .hotels.email,
        .hotels.kind, .hotels.region.country_code, .hotels.region.name, .hotels.postal_code
    ] | @csv' >> $RATEHAWK_HOTELS_CSV"

jq -c '. | {hotels: ., rooms: .room_groups}' "$RATEHAWK_DUMP" | \
    parallel --pipe --block 10M "jq -r '
    select(.rooms) | .rooms[] | [
        .name, (.images | join(",")), .rg_ext, .name_struct.bathroom, .name_struct.bedding_type
    ] | @csv' >> $RATEHAWK_ROOMS_CSV"

# Optimize PostgreSQL Import
$PSQL_EXEC -c "CREATE UNLOGGED TABLE ratehawk_hotels AS SELECT * FROM ratehawk_hotels LIMIT 0;"
$PSQL_EXEC -c "CREATE UNLOGGED TABLE ratehawk_rooms AS SELECT * FROM ratehawk_rooms LIMIT 0;"

$PSQL_EXEC -c "\COPY ratehawk_hotels FROM '$RATEHAWK_HOTELS_CSV' WITH (FORMAT csv, HEADER false);"
$PSQL_EXEC -c "\COPY ratehawk_rooms FROM '$RATEHAWK_ROOMS_CSV' WITH (FORMAT csv, HEADER false);"

$PSQL_EXEC -c "ALTER TABLE ratehawk_hotels SET LOGGED;"
$PSQL_EXEC -c "ALTER TABLE ratehawk_rooms SET LOGGED;"

$PSQL_EXEC -c "ANALYZE ratehawk_hotels;"
$PSQL_EXEC -c "ANALYZE ratehawk_rooms;"

echo "Data import optimized and completed successfully."
