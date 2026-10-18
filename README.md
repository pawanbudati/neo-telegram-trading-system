# Kotak Telegram Trading System

This repository contains two independent Python applications that work together to turn messages from Telegram trading channels into orders through Kotak Neo:

1. **Producer** (`producer/`): listens to configured Telegram chats, asks Gemini to parse each message into a structured signal, and publishes actionable signals to Kafka.
2. **Consumer** (`consumer/`): reads signals from Kafka, resolves the live option contract through Kotak Neo, sizes the order, and manages placement, modification, and cancellation.

Kafka decouples the two applications. They may run on the same machine or on separate machines, provided both can connect to the same Kafka topic. Each application has independent dependencies and configuration.

> **Live trading:** The consumer has no dry-run mode. It can place real orders as soon as it receives an actionable signal. Configure and verify the account, channel, order product, and sizing before starting it.

## Repository layout

```text
kotak-telegram-trading-system/
├── producer/
│   ├── config/          # Producer settings loaded from producer/.env
│   ├── src/             # Telegram listener, Gemini parser, Kafka publisher
│   ├── tests/
│   ├── requirements.txt
│   └── .env.example
├── consumer/
│   ├── config/          # Consumer settings loaded from consumer/.env
│   ├── src/             # Kafka consumer, Kotak client, order manager
│   ├── tests/
│   ├── requirements.txt
│   └── .env.example
├── .gitignore
└── README.md
```

The `.env` files and Telegram session files contain private credentials and are ignored by Git. Keep `.env.example` files as templates; never put real credentials in them.

## Requirements

- Python 3.10 or later.
- Node.js and npm, to install PM2 for background process management.
- A Kafka broker reachable by both producer and consumer. For a local setup, Docker is the easiest option.
- Telegram API credentials and access to the chats to monitor.
- A Gemini API key.
- Kotak Neo API credentials for the account that will place orders.

Install each project's dependencies in its own virtual environment; do not install both requirements files into one environment.

## 1. Start Kafka

For a local development broker with Docker, run this from any directory:

```bash
docker run -d --name trading-kafka -p 9092:9092 apache/kafka:4.1.2
```

Kafka is then available to local processes at `localhost:9092`. If Docker reports that `trading-kafka` already exists, start the existing container instead:

```bash
docker start trading-kafka
```

Check its status and logs with:

```bash
docker ps --filter name=trading-kafka
docker logs -f trading-kafka
```

The applications use `kotak_trading_signals` as the default topic. A broker configured to allow automatic topic creation will create it when the producer first publishes. If automatic topic creation is disabled, create the topic using your Kafka administration tools before starting the applications.

For production or a shared server, use an appropriately secured Kafka deployment. Do not expose a plaintext Kafka listener to an untrusted network. Configure the broker's advertised listeners, firewall, and authentication so remote application machines can reach it.

## 2. Configure and run the producer

From the repository root:

```bash
cd producer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `producer/.env` and set at least:

| Variable | Purpose |
| --- | --- |
| `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` | Telegram application credentials from `my.telegram.org`. |
| `TELEGRAM_PHONE` | Phone number for the Telegram account used to sign in. |
| `TELEGRAM_CHANNELS` | Comma-separated channel usernames, invite links, or chat IDs to monitor. Set specific chats; an empty value listens to all incoming chats available to the account. |
| `GEMINI_API_KEY` | API key used to parse messages with Gemini. |
| `GEMINI_MODEL` | Gemini model name (default in the template: `gemini-3.8-flash`). |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address (default: `localhost:9092`). |
| `KAFKA_TOPIC_SIGNALS` | Shared producer/consumer topic (default: `kotak_trading_signals`). |
| `KAFKA_SECURITY_PROTOCOL` | Kafka security protocol (default: `PLAINTEXT`; use the settings required by your broker). |
| `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` | Optional SASL credentials for a secured Kafka broker. |

The template also includes `GEMINI_TEMPERATURE`, `GEMINI_TIMEOUT_SECONDS`, `MESSAGE_HISTORY_CONTEXT_SIZE`, and `TELEGRAM_SESSION_NAME` to tune parsing context and session naming.

### Sign in to Telegram once

With the producer virtual environment active, run:

```bash
python main.py --login-telegram
```

Complete Telegram's sign-in and verification prompts. The command lists the account's available channels and groups. It creates a local session file in `producer/`; protect it like a password and do not commit or share it. The producer uses that saved session on subsequent runs.

### Check parsing and start publishing

Optionally ask Gemini to parse one sample message without publishing it:

```bash
python main.py --test-signal "buy nifty 22500 above 50"
```

This calls Gemini and may incur API usage. It does not send an order or publish the parsed signal. Start the producer with:

```bash
python main.py
```

It connects to Telegram and Kafka and publishes actionable parsed signals. Review its logs to confirm the expected chats resolve and messages are being handled. If a configured chat cannot be resolved, the producer logs an error and ignores that chat.

Producer CLI options:

```text
--login-telegram        Sign in and create the Telegram session
--test-signal TEXT      Parse one message without publishing
--kafka-servers ADDR    Override KAFKA_BOOTSTRAP_SERVERS for this run
--kafka-topic NAME      Override KAFKA_TOPIC_SIGNALS for this run
```

## 3. Configure and run the consumer

Open another terminal (or use the background instructions below), then from the repository root:

```bash
cd consumer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Edit `consumer/.env` and set the Kotak Neo credentials and Kafka connection:

| Variable | Purpose |
| --- | --- |
| `KOTAK_CONSUMER_KEY` | Kotak Neo consumer key. |
| `KOTAK_MOBILE_NUMBER` | Registered account mobile number. |
| `KOTAK_UCC` | Unique client code for the trading account. |
| `KOTAK_TOTP_KEY` | TOTP secret used for API login. |
| `KOTAK_MPIN` | Kotak Neo MPIN. |
| `KOTAK_ENVIRONMENT` | Kotak API environment (default: `prod`). |
| `KAFKA_BOOTSTRAP_SERVERS` | Address of the same Kafka broker used by the producer. |
| `KAFKA_TOPIC_SIGNALS` | Must match the producer topic (default: `kotak_trading_signals`). |
| `KAFKA_CONSUMER_GROUP_ID` | Default consumer group name (can also be set with `--group-id`). |
| `KAFKA_AUTO_OFFSET_RESET` | Starting offset behavior for a new consumer group (default: `latest`). |
| `KAFKA_SECURITY_PROTOCOL` | Kafka security protocol (default: `PLAINTEXT`). |
| `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` | Optional SASL credentials for a secured Kafka broker. |

Order sizing defaults are `DEFAULT_PRODUCT_TYPE=MIS` and `DEFAULT_LOTS=1`. You can set optional `RISK_PERCENTAGE` and `DEFAULT_CAPITAL` values to size orders based on stop loss and capital. If a risk percentage is configured but capital is omitted, the consumer attempts to retrieve available funds from Kotak. Review these settings carefully before trading.

Start a consumer after verifying all settings:

```bash
python main.py --account-id acc_main --group-id kotak_executor_acc_main
```

The consumer authenticates to Kotak during startup. Missing credentials, authentication failures, or an inability to resolve a live contract prevent normal order execution. When actionable Kafka messages arrive, the consumer places or manages orders through the configured account.

Consumer CLI options:

| Option | Purpose |
| --- | --- |
| `--account-id ID` | Label used in logs (default: `default`). |
| `--group-id NAME` | Kafka consumer group. Use a distinct group for each account that should receive every signal. |
| `--kafka-servers ADDR` | Override the broker address for this run. |
| `--kafka-topic NAME` | Override the topic for this run. |
| `--lots N` | Override default lot count. |
| `--product MIS` or `--product NRML` | Override order product. |
| `--risk-percent PERCENT` | Override risk percentage for risk-based sizing. |
| `--capital AMOUNT` | Override capital used for sizing. |

See the full argument list at any time with `python main.py --help` from the `consumer/` directory.

## Run services in the background with PM2

The root-level `runner.sh` sets up both Python environments and manages the producer and consumer as background processes through PM2. Kafka runs separately; start the broker before starting the applications.

Install PM2 once if it is not already installed:

```bash
sudo npm install -g pm2
```

Make sure `producer/.env` and `consumer/.env` exist and contain the required values. The setup command creates either file from its `.env.example` template if missing, then installs that project's Python requirements:

```bash
chmod +x runner.sh
./runner.sh setup
```

Edit both `.env` files with the settings described above. Then authenticate Telegram interactively once:

```bash
./runner.sh login-telegram
```

Start Kafka, then start both applications under PM2:

```bash
./runner.sh start acc_main kotak_executor_acc_main
```

The runner checks that Telegram, Gemini, and Kotak credentials are present and that the Telegram session exists before starting. If the session has expired, run `./runner.sh login-telegram` again. To perform setup and then attempt startup in one command after filling configuration and creating a Telegram session, use `./runner.sh setup-start acc_main kotak_executor_acc_main`.

PM2 controls both processes in the background. Use these commands from the repository root:

```bash
./runner.sh status
./runner.sh logs                 # Follow logs from both processes
./runner.sh logs producer        # Follow only producer logs
./runner.sh logs consumer        # Follow only consumer logs
./runner.sh stop
./runner.sh restart acc_main kotak_executor_acc_main
./runner.sh delete               # Stop and remove both PM2 entries
```

The runner saves the PM2 process list. To have PM2 restore it after a machine reboot, configure PM2's startup service once with `pm2 startup`, run the command PM2 prints, and then run `pm2 save`. Use `./runner.sh stop` before intentionally stopping Kafka or shutting down the apps.

## Run on separate machines

The producer can run on a machine with Telegram access, while the consumer runs on a machine with the Kotak environment. Both must use the same Kafka broker address, topic, and compatible security settings. For each app, configure `KAFKA_BOOTSTRAP_SERVERS` to an address reachable from that machine, rather than `localhost` when Kafka is elsewhere. Configure the Kafka broker to advertise a reachable address and permit the required network connections; a host firewall or cloud firewall may also need a rule for the broker listener.

To have multiple Kotak accounts receive every signal, run one consumer process per account with a unique `--group-id` for each. Processes in the same Kafka consumer group divide messages among themselves, so do not reuse a group ID when each account needs every signal.

### Add a consumer on another VM

The remote consumer does **not** connect directly to the producer. Both applications connect to Kafka: the producer publishes to `kotak_trading_signals`, and each consumer group reads its own copy of those events.

On the consumer VM, clone the repository, install PM2 if necessary, and set up only the consumer:

```bash
git clone https://github.com/pawanbudati/neo-telegram-trading-system.git
cd neo-telegram-trading-system
sudo npm install -g pm2
chmod +x runner.sh
./runner.sh setup-consumer
```

Edit `consumer/.env` on that VM. Give it that account's Kotak credentials, and set `KAFKA_BOOTSTRAP_SERVERS` to the Kafka VM's reachable private hostname or IP and port, for example `10.0.0.12:9092`. Keep `KAFKA_TOPIC_SIGNALS=kotak_trading_signals` and `KAFKA_SECURITY_PROTOCOL` consistent with the broker and producer. Do not use `localhost:9092` on the consumer VM unless Kafka is running on that same VM.

Then start only the consumer process with a unique account ID and consumer group:

```bash
./runner.sh start-consumer acc_secondary kotak_executor_acc_secondary
./runner.sh status
./runner.sh logs consumer
```

`start-consumer` does not start another producer. Repeat this on each account VM, using that account's credentials and a different group ID each time. The default offset policy is `latest`, so start each new consumer before you expect signals, or set `KAFKA_AUTO_OFFSET_RESET=earliest` if a brand-new group should read retained earlier events too.

The Kafka VM must accept connections from the consumer VM on the broker listener port. With cloud VMs, allow that port only from the consumer VM's private IP or private network. Also configure Kafka's `advertised.listeners` so it tells remote clients to reconnect to a hostname/IP they can reach; setting the consumer's bootstrap address alone is not enough if the broker advertises `localhost` or a private address unreachable from that VM. Kafka documents `advertised.listeners` as the broker endpoints clients use to connect ([broker configuration](https://kafka.apache.org/41/configuration/broker-configs/)). Avoid exposing an unauthenticated plaintext broker to the public internet.

## Tests

Install pytest in the corresponding virtual environment if it is not already available, then run each project's tests from that project's directory:

```bash
cd producer
python -m pip install pytest
python -m pytest
```

```bash
cd consumer
python -m pip install pytest
python -m pytest
```

Consumer order-management tests use a test-only fake client and do not connect to Kotak. Tests do not place live orders.

## Security and operational checklist

- The consumer sends live orders. It has no dry-run mode. Only start it when you intend to trade.
- Check `KOTAK_ENVIRONMENT`, the account credentials, product type, lots, capital, and risk percentage before starting the consumer.
- Restrict `TELEGRAM_CHANNELS` to the intended channels. An empty setting means the account listens to all incoming chats it can access.
- Never commit `.env`, Telegram session files, API keys, MPINs, TOTP secrets, broker credentials, or log files.
- Use authenticated and encrypted Kafka connectivity outside a trusted development machine. Do not expose a PLAINTEXT broker on a public network.
- Monitor both application logs. The producer reports channel and parsing activity; the consumer reports Kafka messages and Kotak execution responses.
