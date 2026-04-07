# ============================================================
#  short_straddle.py
#  Options Strategy: Short Straddle
#
#  Sell ATM CE + Sell ATM PE
#  Direction neutral — profit when underlying stays flat
#
#  Interface:
#  get_legs(underlying_price, contracts_df, sd_move, direction)
#  → returns list of legs
# ============================================================

def get_strategy_info():
    return {
        "name":        "Short Straddle",
        "description": "Sell ATM CE + Sell ATM PE",
        "direction":   "neutral",
        "legs":        2,
    }


def get_legs(underlying_price: float, contracts_df,
             sd_move: float, direction: str) -> list:
    """
    Short Straddle — sell ATM CE and ATM PE.

    underlying_price → current price of stock/index
    contracts_df     → DataFrame with option contracts
    sd_move          → 1 SD move in points (not used here)
    direction        → BULLISH/BEARISH from screener (not used — neutral)

    Returns list of leg dicts:
    [
        {"action": "SELL", "type": "CE", "strike_type": "ATM", "offset": 0},
        {"action": "SELL", "type": "PE", "strike_type": "ATM", "offset": 0},
    ]
    """
    return [
        {
            "action":       "SELL",
            "type":         "CE",
            "strike_type":  "ATM",
            "offset":       0,        # 0 = ATM, positive = OTM
            "target_price": None,     # None = use ATM
        },
        {
            "action":       "SELL",
            "type":         "PE",
            "strike_type":  "ATM",
            "offset":       0,
            "target_price": None,
        },
    ]
