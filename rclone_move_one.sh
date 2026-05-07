#!/bin/bash
# Usage: rclone_move_one.sh <filename> [multi_thread_streams]
#
# Moves a single file from the user's My Drive (GDG:/Downloads/<filename>)
# to the local NAS download path.  Designed to be invoked many times in
# parallel — each invocation handles exactly one file, so concurrent
# rclone processes never race on the same source.

NAME="$1"
STREAMS="${2:-4}"

if [ -z "$NAME" ]; then
    echo "Usage: $0 <filename> [multi_thread_streams]"
    exit 1
fi

SRC="GDG:/Downloads/$NAME"
DST="/volume1/MK/Downloads/"

echo "=== rclone_move_one start: $NAME (streams=$STREAMS) ==="
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
