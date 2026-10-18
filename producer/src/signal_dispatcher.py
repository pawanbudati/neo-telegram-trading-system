import asyncio
import inspect
from typing import Callable, Dict, Any, Optional, Union
from src.logger import logger
from src.models import ParsedSignal

class SignalDispatcher:
    """
    Decoupled Signal Dispatcher (Pub/Sub Event Bus).
    
    Accepts ParsedSignals from signal producers (like TelegramListener or test scripts)
    and broadcasts them to all registered subscribers (e.g., one or more Kotak account executors).
    
    Ensures failure or slowness in one account's order execution does not crash
    or prevent execution in other accounts.
    """

    def __init__(self):
        self._subscribers: Dict[str, Callable[[ParsedSignal], Any]] = {}

    def register(self, name: str, handler: Callable[[ParsedSignal], Any]):
        """
        Registers a subscriber callback or handler.
        Handler can be a synchronous or asynchronous function taking (signal: ParsedSignal).
        """
        self._subscribers[name] = handler
        logger.info(f"[cyan]SignalDispatcher: Registered subscriber '{name}'[/cyan]")

    def unregister(self, name: str):
        """Unregisters a subscriber by name."""
        if name in self._subscribers:
            del self._subscribers[name]
            logger.info(f"[cyan]SignalDispatcher: Unregistered subscriber '{name}'[/cyan]")

    def add_order_manager(self, order_manager, name: Optional[str] = None):
        """Helper to register an OrderManager instance directly."""
        mgr_name = name or getattr(order_manager, "account_id", f"account_{len(self._subscribers) + 1}")
        self.register(mgr_name, order_manager.process_signal)

    async def dispatch(self, signal: ParsedSignal) -> Dict[str, Any]:
        """
        Broadcasts a ParsedSignal to all registered subscribers.
        Returns a dict mapping subscriber_name -> result/error.
        """
        if not self._subscribers:
            logger.warning("[yellow]SignalDispatcher: No subscribers registered to handle signal.[/yellow]")
            return {}

        results: Dict[str, Any] = {}

        async def _call_handler(name: str, handler: Callable[[ParsedSignal], Any]):
            try:
                if inspect.iscoroutinefunction(handler):
                    res = await handler(signal)
                else:
                    res = handler(signal)
                return name, {"success": True, "result": res}
            except Exception as e:
                logger.error(f"[bold red]SignalDispatcher: Error in subscriber '{name}':[/bold red] {e}", exc_info=True)
                return name, {"success": False, "error": str(e)}

        # Execute handlers concurrently
        tasks = [_call_handler(name, handler) for name, handler in self._subscribers.items()]
        completed = await asyncio.gather(*tasks)

        for name, outcome in completed:
            results[name] = outcome

        return results

    def dispatch_sync(self, signal: ParsedSignal) -> Dict[str, Any]:
        """
        Synchronous dispatch convenience helper (useful in non-async contexts or tests).
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If already in an event loop, create a new task or run synchronously
                results = {}
                for name, handler in self._subscribers.items():
                    try:
                        if inspect.iscoroutinefunction(handler):
                            # Cannot block running loop for coroutine, run via task
                            task = asyncio.create_task(handler(signal))
                            results[name] = {"success": True, "task": task}
                        else:
                            res = handler(signal)
                            results[name] = {"success": True, "result": res}
                    except Exception as e:
                        results[name] = {"success": False, "error": str(e)}
                return results
            else:
                return loop.run_until_complete(self.dispatch(signal))
        except RuntimeError:
            return asyncio.run(self.dispatch(signal))
