#!/usr/bin/env bash
# For a local RabbitMQ installation managed by systemd.
# Usage: sudo ./start-or-reset-rabbitmq.sh
set -euo pipefail

service_name=rabbitmq-server

if [[ ${EUID} -ne 0 ]]; then
  printf 'Run as root: sudo %s\n' "$0" >&2
  exit 1
fi

for command_name in systemctl rabbitmqctl rabbitmq-diagnostics; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'Required command not found: %s\n' "$command_name" >&2
    exit 1
  fi
done

if ! systemctl cat "$service_name" >/dev/null 2>&1; then
  printf 'systemd unit not found: %s\n' "$service_name" >&2
  exit 1
fi

if ! systemctl is-active --quiet "$service_name"; then
  printf 'RabbitMQ service is stopped. Starting it...\n'
  systemctl start "$service_name"
  rabbitmq-diagnostics -q check_running
  printf 'RabbitMQ is running.\n'
  exit 0
fi

# A failed CLI check can mean a cookie or authentication problem, not a stopped node.
# Do not use that failure as a reason to reset the broker.
if ! rabbitmq-diagnostics -q check_running; then
  printf 'The service is active, but the RabbitMQ CLI could not verify the node.\n' >&2
  printf 'Check the Erlang cookie, node name, and service logs; no reset was performed.\n' >&2
  exit 1
fi

printf 'RabbitMQ is running. Reset will delete its local broker data, including messages, queues, users, and virtual hosts.\n'
printf 'Stopping RabbitMQ application...\n'
rabbitmqctl stop_app
printf 'Resetting RabbitMQ node...\n'
rabbitmqctl reset
printf 'Starting RabbitMQ application...\n'
rabbitmqctl start_app
rabbitmq-diagnostics -q check_running
printf 'RabbitMQ reset completed and application is running.\n'
