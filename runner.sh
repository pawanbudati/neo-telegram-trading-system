#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PRODUCER_DIR="$ROOT_DIR/producer"
CONSUMER_DIR="$ROOT_DIR/consumer"
ECOSYSTEM_FILE="$ROOT_DIR/ecosystem.config.cjs"
PRODUCER_NAME="kotak-telegram-producer"
CONSUMER_NAME="kotak-telegram-consumer"

usage() {
  cat <<'USAGE'
Usage: ./runner.sh COMMAND [ACCOUNT_ID] [GROUP_ID]

Commands:
  setup                 Create project virtual environments, install requirements,
                        and create missing .env files from the example templates.
  setup-producer        Set up only the producer environment and requirements.
  setup-consumer        Set up only the consumer environment and requirements.
  login-telegram        Sign in to Telegram interactively and save its session.
  start [ACCOUNT] [GROUP]
                        Start/restart the producer and consumer under PM2.
  start-producer        Start/restart only the producer under PM2.
  start-consumer [ACCOUNT] [GROUP]
                        Start/restart only the consumer under PM2.
  setup-start [ACCOUNT] [GROUP]
                        Run setup, then start both services under PM2.
  stop                  Stop both PM2 processes.
  stop-producer         Stop only the producer.
  stop-consumer         Stop only the consumer.
  restart [ACCOUNT] [GROUP]
                        Restart both services under PM2.
  status                Show PM2 process status.
  logs [producer|consumer]
                        Follow PM2 logs (both services by default).
  delete                Stop and remove both services from PM2.

ACCOUNT_ID defaults to "default". GROUP_ID defaults to "kotak_executor_<ACCOUNT_ID>".
Kafka must be running separately. Fill in both .env files before starting services.
USAGE
}

require_pm2() {
  if ! command -v pm2 >/dev/null 2>&1; then
    printf 'PM2 is required but was not found. Install it with: npm install -g pm2\n' >&2
    exit 1
  fi
}

setup_project() {
  local project_dir="$1"
  printf '\nSetting up %s...\n' "$(basename "$project_dir")"

  if [[ ! -f "$project_dir/.env" ]]; then
    cp "$project_dir/.env.example" "$project_dir/.env"
    printf 'Created %s/.env from the example template.\n' "$(basename "$project_dir")"
  fi

  python3 -m venv "$project_dir/.venv"
  "$project_dir/.venv/bin/python" -m pip install -r "$project_dir/requirements.txt"
}

read_env_value() {
  local env_file="$1"
  local key="$2"
  local value
  value="$(awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$env_file")"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  value="${value#\"}"; value="${value%\"}"
  value="${value#\'}"; value="${value%\'}"
  printf '%s' "$value"
}

require_env_value() {
  local env_file="$1"
  local key="$2"
  local value
  value="$(read_env_value "$env_file" "$key")"
  if [[ -z "$value" || "${value,,}" == *your_* ]]; then
    printf 'Set %s in %s before starting services.\n' "$key" "$env_file" >&2
    return 1
  fi
}

check_producer_configuration() {
  local producer_env="$PRODUCER_DIR/.env"
  local session_name
  local failed=0

  [[ -f "$producer_env" ]] || { printf 'Missing %s. Run ./runner.sh setup first.\n' "$producer_env" >&2; failed=1; }
  (( failed == 0 )) || return 1

  for key in TELEGRAM_API_ID TELEGRAM_API_HASH TELEGRAM_PHONE TELEGRAM_CHANNELS GEMINI_API_KEY; do
    require_env_value "$producer_env" "$key" || failed=1
  done

  session_name="$(read_env_value "$producer_env" TELEGRAM_SESSION_NAME)"
  session_name="${session_name:-kotak_trader_session}"
  if [[ ! -f "$PRODUCER_DIR/$session_name.session" ]]; then
    printf 'Telegram session is missing. Run ./runner.sh login-telegram first.\n' >&2
    failed=1
  fi

  if (( failed != 0 )); then
    printf 'Producer configuration is incomplete. Update producer/.env and retry.\n' >&2
    return 1
  fi
}

check_consumer_configuration() {
  local consumer_env="$CONSUMER_DIR/.env"
  local failed=0

  [[ -f "$consumer_env" ]] || { printf 'Missing %s. Run ./runner.sh setup-consumer first.\n' "$consumer_env" >&2; failed=1; }
  (( failed == 0 )) || return 1

  for key in KOTAK_CONSUMER_KEY KOTAK_MOBILE_NUMBER KOTAK_UCC KOTAK_TOTP_KEY KOTAK_MPIN; do
    require_env_value "$consumer_env" "$key" || failed=1
  done

  if (( failed != 0 )); then
    printf 'Consumer configuration is incomplete. Update consumer/.env and retry.\n' >&2
    return 1
  fi
}

check_start_configuration() {
  check_producer_configuration
  check_consumer_configuration
}

start_services() {
  local account_id="${1:-${KOTAK_ACCOUNT_ID:-default}}"
  local group_id="${2:-${KOTAK_GROUP_ID:-kotak_executor_${account_id}}}"
  require_pm2
  check_start_configuration

  export KOTAK_ACCOUNT_ID="$account_id"
  export KOTAK_GROUP_ID="$group_id"
  pm2 startOrRestart "$ECOSYSTEM_FILE" --update-env
  pm2 save
  printf '\nServices started in the background. Use ./runner.sh status or ./runner.sh logs.\n'
}

start_producer() {
  require_pm2
  check_producer_configuration
  pm2 startOrRestart "$ECOSYSTEM_FILE" --only "$PRODUCER_NAME" --update-env
  pm2 save
  printf '\nProducer started in the background. Use ./runner.sh status or ./runner.sh logs producer.\n'
}

start_consumer() {
  local account_id="${1:-${KOTAK_ACCOUNT_ID:-default}}"
  local group_id="${2:-${KOTAK_GROUP_ID:-kotak_executor_${account_id}}}"
  require_pm2
  check_consumer_configuration
  export KOTAK_ACCOUNT_ID="$account_id"
  export KOTAK_GROUP_ID="$group_id"
  pm2 startOrRestart "$ECOSYSTEM_FILE" --only "$CONSUMER_NAME" --update-env
  pm2 save
  printf '\nConsumer %s started in the background with group %s.\n' "$account_id" "$group_id"
}

command_name="${1:-help}"
shift || true

case "$command_name" in
  setup)
    setup_project "$PRODUCER_DIR"
    setup_project "$CONSUMER_DIR"
    printf '\nSetup complete. Edit producer/.env and consumer/.env, then run ./runner.sh login-telegram.\n'
    ;;
  setup-producer)
    setup_project "$PRODUCER_DIR"
    printf '\nProducer setup complete. Edit producer/.env, then run ./runner.sh login-telegram.\n'
    ;;
  setup-consumer)
    setup_project "$CONSUMER_DIR"
    printf '\nConsumer setup complete. Edit consumer/.env before starting it.\n'
    ;;
  login-telegram)
    [[ -x "$PRODUCER_DIR/.venv/bin/python" ]] || { printf 'Run ./runner.sh setup first.\n' >&2; exit 1; }
    require_env_value "$PRODUCER_DIR/.env" TELEGRAM_API_ID
    require_env_value "$PRODUCER_DIR/.env" TELEGRAM_API_HASH
    require_env_value "$PRODUCER_DIR/.env" TELEGRAM_PHONE
    (cd "$PRODUCER_DIR" && exec .venv/bin/python main.py --login-telegram)
    ;;
  start)
    start_services "${1:-}" "${2:-}"
    ;;
  start-producer)
    start_producer
    ;;
  start-consumer)
    start_consumer "${1:-}" "${2:-}"
    ;;
  setup-start)
    setup_project "$PRODUCER_DIR"
    setup_project "$CONSUMER_DIR"
    start_services "${1:-}" "${2:-}"
    ;;
  stop)
    require_pm2
    pm2 stop "$PRODUCER_NAME" "$CONSUMER_NAME"
    ;;
  stop-producer)
    require_pm2
    pm2 stop "$PRODUCER_NAME"
    ;;
  stop-consumer)
    require_pm2
    pm2 stop "$CONSUMER_NAME"
    ;;
  restart)
    start_services "${1:-}" "${2:-}"
    ;;
  status)
    require_pm2
    pm2 status
    ;;
  logs)
    require_pm2
    case "${1:-all}" in
      producer) pm2 logs "$PRODUCER_NAME" ;;
      consumer) pm2 logs "$CONSUMER_NAME" ;;
      all) pm2 logs "$PRODUCER_NAME" "$CONSUMER_NAME" ;;
      *) usage >&2; exit 2 ;;
    esac
    ;;
  delete)
    require_pm2
    pm2 delete "$PRODUCER_NAME" "$CONSUMER_NAME"
    pm2 save
    ;;
  help|-h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
