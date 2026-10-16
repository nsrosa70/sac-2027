#!/usr/bin/env bash
# Local Apache Pulsar standalone, launched with bin/pulsar-daemon.
# Usage: ./start-or-reset-pulsar.sh /absolute/path/to/apache-pulsar
# If running: stop, delete local data, start fresh. If stopped: start without deleting data.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  printf 'Usage: %s /absolute/path/to/apache-pulsar\n' "$0" >&2
  exit 2
fi
pulsar_home=$(cd -- "$1" && pwd -P)
daemon="$pulsar_home/bin/pulsar-daemon"
[[ -x "$daemon" ]] || { printf 'Not executable: %s\n' "$daemon" >&2; exit 1; }

# pulsar-daemon also reads this file when locating its PID file.
if [[ -f "$pulsar_home/conf/pulsar_env.sh" ]]; then
  # shellcheck source=/dev/null
  source "$pulsar_home/conf/pulsar_env.sh"
fi
pid_dir=${PULSAR_PID_DIR:-"$pulsar_home/bin"}
pid_file="$pid_dir/pulsar-standalone.pid"
data_dir="$pulsar_home/data"

if [[ -e "$pid_file" ]]; then
  pid=$(<"$pid_file")
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || { printf 'Invalid PID file: %s\n' "$pid_file" >&2; exit 1; }
  if kill -0 "$pid" 2>/dev/null; then
    args=$(ps -p "$pid" -o args=)
    [[ "$args" == *PulsarStandalone* ]] || {
      printf 'PID %s is not a Pulsar standalone process; aborting.\n' "$pid" >&2
      exit 1
    }
    printf 'Stopping running Pulsar standalone (PID %s)...\n' "$pid"
    "$daemon" stop standalone
    for ((i=0; i<60; i++)); do
      if ! kill -0 "$pid" 2>/dev/null; then break; fi
      sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
      printf 'Pulsar did not stop. Data was not changed.\n' >&2
      exit 1
    fi
    [[ ! -L "$data_dir" ]] || { printf 'Data path is a symlink; aborting: %s\n' "$data_dir" >&2; exit 1; }
    if [[ -d "$data_dir" ]]; then
      printf 'Deleting standalone data: %s\n' "$data_dir"
      rm -rf -- "$data_dir"
    fi
  else
    printf 'Stale PID file found; refusing to guess whether another instance is running: %s\n' "$pid_file" >&2
    exit 1
  fi
fi

printf 'Starting Pulsar standalone...\n'
"$daemon" start standalone
printf 'Start command completed. Check %s/logs for startup status.\n' "$pulsar_home"
