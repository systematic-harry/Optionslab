# ============================================================
#  short_strangle_1sd.py
#  Options Strategy: Short Strangle (1 Standard Deviation)
#
#  Sell CE at price + 1SD
#  Sell PE at price - 1SD
#
#  Direction neutral — profit when underlying stays
#  within 1 SD range till expiry
# ============================================================

def get_strategy_info():
    return {
        "name":        "Short Strangle 1SD",
        "description": "Sell CE at +1SD, Sell PE at -1SD",
        "direction":   "neutral",
        "legs":        2,
    }


def get_legs(underlying_price: float, contracts_df,
             sd_move: float, direction: str) -> list:
    """
    Short Strangle 1SD — sell OTM CE and OTM PE at 1SD strikes.

    CE target = underlying_price + sd_move
    PE target = underlying_price - sd_move
    """
    return [
        {
            "action":       "SELL",
            "type":         "CE",
            "strike_type":  "OTM",
            "offset":       0,
            "target_price": round(underlying_price + sd_move, 2),
        },
        {
            "action":       "SELL",
            "type":         "PE",
            "strike_type":  "OTM",
            "offset":       0,
            "target_price": round(underlying_price - sd_move, 2),
        },
    ]
