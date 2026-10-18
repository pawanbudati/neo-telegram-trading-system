import os
import asyncio
from typing import List, Optional, Union, Callable, Any
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat

from config.settings import settings
from src.logger import logger
from src.gemini_parser import GeminiParser
from src.signal_dispatcher import SignalDispatcher
from src.models import ParsedSignal

class TelegramListener:
    """
    Listens to configured Telegram channels using Telethon,
    maintains conversation history context, and parses signals with Gemini.
    Broadcasts parsed signals to a decoupled SignalDispatcher (supporting multiple accounts).
    """

    def __init__(
        self,
        gemini_parser: Optional[GeminiParser] = None,
        dispatcher: Optional[SignalDispatcher] = None,
        on_signal: Optional[Callable[[ParsedSignal], Any]] = None
    ):
        self.parser = gemini_parser or GeminiParser()
        self.history: List[str] = []
        self.client: Optional[TelegramClient] = None
        self.monitored_chat_ids: List[int] = []

        # Decoupled dispatcher (Pub/Sub)
        self.dispatcher = dispatcher or SignalDispatcher()

        # Producer only parses and publishes signals; order execution lives in consumer/.

        if on_signal is not None:
            self.dispatcher.register("on_signal_callback", on_signal)

    def _init_client(self) -> TelegramClient:
        if not settings.telegram_api_id or not settings.telegram_api_hash:
            raise ValueError("TELEGRAM_API_ID and TELEGRAM_API_HASH must be configured in .env")

        session_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            f"{settings.telegram_session_name}.session"
        )
        return TelegramClient(
            session=session_path,
            api_id=settings.telegram_api_id,
            api_hash=settings.telegram_api_hash
        )

    async def start(self):
        """Starts the Telegram client and event listener."""
        self.client = self._init_client()

        logger.info("[cyan]Connecting to Telegram client...[/cyan]")
        await self.client.connect()

        if not await self.client.is_user_authorized():
            logger.warning(
                "[bold yellow]Telegram session is not authorized yet![/bold yellow]\n"
                "Please run: [green]python login_telegram.py[/green] or [green]python main.py --login-telegram[/green] "
                "to log in and create your Telegram session."
            )
            return

        user = await self.client.get_me()
        logger.info(f"[bold green]Connected to Telegram as:[/bold green] {user.first_name} ({user.phone or settings.telegram_phone})")

        # Resolve monitored channels
        await self._resolve_channels()

        # Register message handler
        @self.client.on(events.NewMessage)
        async def on_new_message(event):
            await self._handle_incoming_message(event)

        logger.info("[bold green]Started listening for Telegram trading signals...[/bold green]")
        logger.info(f"Monitoring channels: {settings.telegram_channels or 'ALL CHANNELS (TELEGRAM_CHANNELS is empty in .env)'}")

        # Keep running
        await self.client.run_until_disconnected()

    async def _resolve_channels(self):
        """Resolves channel IDs from username, invite link, or ID string."""
        self.monitored_chat_ids = []
        if not settings.telegram_channels:
            logger.warning("[yellow]No TELEGRAM_CHANNELS specified in .env. Will listen to all incoming channel messages.[/yellow]")
            return

        for ch in settings.telegram_channels:
            ch_clean = ch.strip()
            if not ch_clean:
                continue
            try:
                # If numeric string e.g. "-100123456789"
                if ch_clean.lstrip("-").isdigit():
                    entity = await self.client.get_entity(int(ch_clean))
                else:
                    entity = await self.client.get_entity(ch_clean)

                self.monitored_chat_ids.append(entity.id)
                logger.info(f"Successfully joined/resolved channel: [cyan]{getattr(entity, 'title', ch_clean)}[/cyan] (ID: {entity.id})")
            except Exception as e:
                logger.error(f"[bold red]Failed to resolve channel '{ch_clean}':[/bold red] {e}")

    async def _handle_incoming_message(self, event):
        """Processes each incoming message."""
        try:
            chat = await event.get_chat()
            chat_id = getattr(chat, "id", None)
            chat_title = getattr(chat, "title", getattr(chat, "username", "Direct/Unknown"))

            # Filter channels if specific channels are configured
            if settings.telegram_channels and chat_id not in self.monitored_chat_ids:
                return

            text = event.raw_text or ""
            if not text.strip():
                return

            logger.info(
                f"\n[bold magenta]----- INCOMING TELEGRAM MESSAGE -----[/bold magenta]\n"
                f"[dim]Channel: {chat_title} (ID: {chat_id})[/dim]\n"
                f"[bold white]{text}[/bold white]\n"
                f"[dim]------------------------------------[/dim]"
            )

            # Process with Gemini
            parsed_signal = self.parser.parse_message(text, history=self.history)

            # Record in history buffer for context memory
            self.history.append(f"[{chat_title}] {text}")
            if len(self.history) > 20:
                self.history.pop(0)

            # If trading signal detected, broadcast via decoupled SignalDispatcher
            if parsed_signal.is_trading_signal:
                results = await self.dispatcher.dispatch(parsed_signal)
                logger.info(f"[bold green]Signal Dispatch Results:[/bold green] {results}")

        except Exception as e:
            logger.error(f"[bold red]Error in message event handler:[/bold red] {e}", exc_info=True)
