#!/bin/bash
# Usage: rclone_move_one.sh <filename> [multi_thread_streams] [dest_path]
#
# Moves a single file from the user's My Drive (GDG:/Downloads/<filename>)
# to a NAS local directory.  Designed to be invoked many times in
# parallel — each invocation handles exactly one file, so concurrent
# rclone processes never race on the same source.

NAME="$1"
STREAMS="${2:-4}"
DST="${3:-/volume1/MK/Downloads/}"

if [ -z "$NAME" ]; then
    echo "Usage: $0 <filename> [streams] [dest_path]"
    exit 1
fi

mkdir -p "$DST"

SRC="GDG:/Downloads/$NAME"

echo "=== rclone_move_one start: $NAME -> $DST (streams=$STREAMS) ==="
rclone move "$SRC" "$DST" \
    --config /volume1/MK/rclone.conf \
    --log-level INFO \
    --stats 1s \
    --stats-file-name-length 0 \
    --transfers=1 \
    --multi-thread-streams="$STREAMS" \
    --multi-thread-cutoff=64M \
    --buffer-size=64M \
    --checkers=8 \
    --drive-chunk-size=256M \
    --drive-use-trash=false
RC=$?
echo "=== rclone_move_one done: $NAME (rc=$RC) ==="
exit $RC
