#!/bin/sh
# Allow concurrently running storage tests to hold their WALs and SSTs open.
# Usage: sh tools/with-test-fds.sh cargo +stable test --test integration
set -eu

preferred=4096
# The soft/hard flags are shell extensions. Probe both before changing a limit.
if soft=$(command ulimit -S -n 2>/dev/null) &&
    hard=$(command ulimit -H -n 2>/dev/null) &&
    [ "$soft" != unlimited ] && [ "$soft" -lt "$preferred" ]; then
    target=$preferred
    if [ "$hard" != unlimited ] && [ "$hard" -lt "$target" ]; then
        target=$hard
    fi
    # An external ulimit may succeed without changing this shell's limit.
    if [ "$soft" -lt "$target" ] && command ulimit -S -n "$target" 2>/dev/null &&
        effective=$(command ulimit -S -n 2>/dev/null); then
        soft=$effective
    fi
    # With a low ceiling, avoid adding up independently open test databases.
    # Keep an explicit libtest concurrency setting supplied by the caller.
    if [ "$soft" -lt "$preferred" ] && [ -z "${RUST_TEST_THREADS:-}" ]; then
        export RUST_TEST_THREADS=1
    fi
fi
exec "$@"
