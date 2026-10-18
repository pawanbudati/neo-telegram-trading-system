import pytest
from src.gemini_parser import GeminiParser
from src.models import SignalType, OrderCondition

@pytest.fixture
def parser():
    return GeminiParser()

def test_parse_buy_above(parser):
    msg = "buy nifty 22500 above 50"
    parsed = parser.parse_message(msg)
    assert parsed.is_trading_signal is True
    assert parsed.index == "NIFTY"
    assert parsed.strike_price == 22500
    assert parsed.option_type in ("CE", "PE")
    assert parsed.order_condition == OrderCondition.ABOVE
    assert parsed.entry_price == 50.0

def test_parse_price_update(parser):
    history = [
        "Buy NIFTY 22500 CE above 50",
        "wait for trigger"
    ]
    msg = "nifty 22500 abv 55"
    parsed = parser.parse_message(msg, history=history)
    assert parsed.is_trading_signal is True
    assert parsed.index == "NIFTY"
    assert parsed.strike_price == 22500
    assert parsed.option_type == "CE"
    assert parsed.order_condition == OrderCondition.ABOVE
    assert parsed.entry_price == 55.0

def test_parse_ignore_with_context(parser):
    history = [
        "Buy NIFTY 22500 CE above 50",
        "wait for trigger"
    ]
    msg = "ignore"
    parsed = parser.parse_message(msg, history=history)
    assert parsed.is_trading_signal is True
    assert parsed.signal_type in (SignalType.IGNORE_SIGNAL, SignalType.CANCEL_ORDER)
    assert parsed.action == "IGNORE"

def test_parse_sensex_put(parser):
    msg = "buy sensex 75000 pe at 120"
    parsed = parser.parse_message(msg)
    assert parsed.is_trading_signal is True
    assert parsed.index == "SENSEX"
    assert parsed.strike_price == 75000
    assert parsed.option_type == "PE"
    assert parsed.entry_price == 120.0

def test_parse_noise(parser):
    msg = "Good morning everyone, keep stop loss tight today"
    parsed = parser.parse_message(msg)
    assert parsed.is_trading_signal is False
