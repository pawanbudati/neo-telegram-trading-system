#!/usr/bin/env python3
import argparse
import asyncio
import os

from config.settings import settings
from src.gemini_parser import GeminiParser
from src.kafka.producer import KafkaSignalProducer
from src.logger import logger
from src.signal_dispatcher import SignalDispatcher
from src.telegram_listener import TelegramListener


async def run_producer(servers: str | None = None, topic: str | None = None):
    producer = KafkaSignalProducer(bootstrap_servers=servers, topic=topic)
    await producer.start()
    dispatcher = SignalDispatcher()
    dispatcher.register("kafka_publisher", producer.publish)
    listener = TelegramListener(dispatcher=dispatcher)
    try:
        await listener.start()
    finally:
        await producer.stop()


async def login_telegram():
    from telethon import TelegramClient

    if not settings.telegram_api_id or not settings.telegram_api_hash:
        raise ValueError("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in producer/.env")
    session_path = os.path.join(os.path.dirname(__file__), f"{settings.telegram_session_name}.session")
    client = TelegramClient(session_path, settings.telegram_api_id, settings.telegram_api_hash)
    try:
        await client.start(phone=settings.telegram_phone)
        print("Telegram login successful. Available channels and groups:")
        async for dialog in client.iter_dialogs():
            if dialog.is_channel or dialog.is_group:
                username = getattr(dialog.entity, "username", None)
                print(f"{dialog.name}: id={dialog.id}, username={'@' + username if username else '(none)'}")
    finally:
        await client.disconnect()


async def test_signal(text: str):
    parsed = GeminiParser().parse_message(text)
    print(parsed.model_dump_json(indent=2))


def main():
    parser = argparse.ArgumentParser(description="Telegram/Gemini Kafka signal producer")
    parser.add_argument("--kafka-servers", default=None)
    parser.add_argument("--kafka-topic", default=None)
    parser.add_argument("--login-telegram", action="store_true")
    parser.add_argument("--test-signal", help="Parse a message with Gemini without publishing it")
    args = parser.parse_args()
    try:
        if args.login_telegram:
            asyncio.run(login_telegram())
        elif args.test_signal:
            asyncio.run(test_signal(args.test_signal))
        else:
            asyncio.run(run_producer(args.kafka_servers, args.kafka_topic))
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")


if __name__ == "__main__":
    main()
