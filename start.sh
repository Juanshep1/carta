#!/usr/bin/env bash
# Convenience wrapper — forwards to backend/start.sh
exec "$(dirname "$0")/backend/start.sh" "$@"
