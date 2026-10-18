import json
from typing import List, Optional
from google import genai
from google.genai import types

from config.settings import settings
from src.logger import logger
from src.models import ParsedSignal, SignalType, OrderCondition

SYSTEM_PROMPT = """
You are an expert quantitative trading parser specialized in Indian stock market derivative signals (NSE / BSE options for NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX, BANKEX).

Your job is to parse incoming Telegram messages from signal channels and output a clean, strict JSON response.

Trading signals may appear in short, informal, or noisy formats, such as:
- "buy nifty 22500 above 50" -> NEW_ORDER, BUY, NIFTY, 22500, CE (if not specified, infer from context or default to CE), ABOVE, trigger=50, entry=50
- "nifty 22500 abv 55 sl 45" -> NEW_ORDER, BUY, NIFTY, 22500, CE, ABOVE, trigger=55, entry=55, stop_loss=45
- "buy sensex 75000 pe at 120" -> NEW_ORDER, BUY, SENSEX, 75000, PE, AT, entry=120
- "ignore", "ignore previous", "cancel", "avoid", "exit", "exit nifty", "cancel nifty 22500" -> CANCEL_ORDER, IGNORE_SIGNAL, or CLOSE_POSITION. action="CANCEL", "IGNORE", or "EXIT". If contract is not mentioned, identify the latest contract from Recent Message History.
- "sl hit", "target 1 done", "good morning" -> is_trading_signal: false, signal_type: NONE.

Allowed JSON structure:
{
  "is_trading_signal": bool,
  "signal_type": "NEW_ORDER" | "MODIFY_ORDER" | "CANCEL_ORDER" | "IGNORE_SIGNAL" | "CLOSE_POSITION" | "NONE",
  "action": "BUY" | "SELL" | "CANCEL" | "MODIFY" | "IGNORE" | "EXIT" | null,
  "index": "NIFTY" | "SENSEX" | "BANKNIFTY" | "FINNIFTY" | "MIDCPNIFTY" | "BANKEX" | null,
  "strike_price": number or null,
  "option_type": "CE" | "PE" | null,
  "order_condition": "ABOVE" | "BELOW" | "AT" | "MARKET" | "LIMIT" | null,
  "entry_price": number or null,
  "trigger_price": number or null,
  "stop_loss": number or null,
  "target": number or null,
  "reasoning": string
}

Key Rules:
1. 'abv' or 'above' => order_condition='ABOVE'.
2. If order_condition is 'ABOVE', entry_price and trigger_price should be set to that trigger value.
3. If 'sl' or 'stoploss' or 'stop loss' is mentioned, extract the numeric value into stop_loss.
4. If no CE/PE is explicitly mentioned, look at the recent message history for the matching strike. If still ambiguous, default option_type to 'CE' and mention this in reasoning.
5. For 'ignore', 'cancel', or 'exit', identify which contract or index from recent history it refers to so it can be cancelled/exited.
6. Return ONLY valid JSON.
"""


class GeminiParser:
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self.api_key = api_key or settings.gemini_api_key
        self.model = model or settings.gemini_model
        self.temperature = settings.gemini_temperature
        self.timeout = settings.gemini_timeout_seconds
        self.client = genai.Client(api_key=self.api_key)

    def parse_message(self, message_text: str, history: Optional[List[str]] = None) -> ParsedSignal:
        """
        Parses an incoming Telegram message using Gemini AI with rolling history context.
        """
        if not message_text or not message_text.strip():
            return ParsedSignal(
                is_trading_signal=False,
                signal_type=SignalType.NONE,
                reasoning="Empty message received",
                raw_message=message_text
            )

        history_text = ""
        if history and len(history) > 0:
            history_lines = [f"- {h}" for h in history[-settings.message_history_context_size:]]
            history_text = "\nRecent Message History:\n" + "\n".join(history_lines) + "\n"

        prompt = f"""{SYSTEM_PROMPT}
{history_text}
Current Message to analyze:
\"\"\"{message_text.strip()}\"\"\"
"""

        try:
            config = types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=self.temperature,
            )
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt,
                config=config
            )

            response_content = response.text.strip()
            data = json.loads(response_content)

            # Map raw fields to ParsedSignal model
            signal_type_str = data.get("signal_type", "NONE")
            try:
                signal_type = SignalType(signal_type_str)
            except ValueError:
                signal_type = SignalType.NONE

            condition_str = data.get("order_condition")
            condition = None
            if condition_str:
                try:
                    condition = OrderCondition(condition_str)
                except ValueError:
                    condition = None

            parsed = ParsedSignal(
                is_trading_signal=bool(data.get("is_trading_signal", False)),
                signal_type=signal_type,
                action=data.get("action"),
                index=data.get("index"),
                strike_price=float(data["strike_price"]) if data.get("strike_price") is not None else None,
                option_type=data.get("option_type"),
                order_condition=condition,
                entry_price=float(data["entry_price"]) if data.get("entry_price") is not None else None,
                trigger_price=float(data["trigger_price"]) if data.get("trigger_price") is not None else None,
                stop_loss=float(data["stop_loss"]) if data.get("stop_loss") is not None else None,
                target=float(data["target"]) if data.get("target") is not None else None,
                reasoning=data.get("reasoning", ""),
                raw_message=message_text
            )

            # Extra fallback for option_type: if not specified and index/strike are present, default to CE
            if parsed.is_trading_signal and parsed.strike_price and not parsed.option_type:
                parsed.option_type = "CE"
                parsed.reasoning += " (Defaulted option_type to CE as none was specified)"

            logger.info(
                f"[bold cyan]Gemini Parsed:[/bold cyan] Signal={parsed.signal_type.value} | "
                f"Action={parsed.action} | Index={parsed.index} | Strike={parsed.strike_price} | "
                f"Option={parsed.option_type} | Price={parsed.entry_price} | Cond={parsed.order_condition}"
            )
            return parsed

        except Exception as e:
            logger.error(f"[bold red]Gemini parsing error:[/bold red] {e}", exc_info=True)
            return ParsedSignal(
                is_trading_signal=False,
                signal_type=SignalType.NONE,
                reasoning=f"Parsing error: {str(e)}",
                raw_message=message_text
            )
