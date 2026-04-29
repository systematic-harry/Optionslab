# ============================================================
#  pdf_exporter.py
#  Goes in: D:\optionlab\scripts\pdf_exporter.py
#
#  Generates screener result PDFs for OptionLab.
#  Zero re-downloading — all data comes from server.py scan results.
#
#  Entry point:
#    generate_pdf(screener_type, stocks, filters_applied) -> bytes
#
#  Called by: POST /export-pdf in server.py
#
#  Data contract — each stock dict must contain:
#    symbol    : str  (display name)
#    ticker    : str
#    signal    : str  (STRONG / WATCH / SKIP)
#    price     : float
#    chart     : list of {date, open, high, low, close, volume}  — 60 bars
#    extras    : dict with screener-specific values (flattened into the row by server.py)
#
#  RS Ratio screener additionally provides:
#    rs_ratio_10_series  : list of floats (60 values)
#    rs_mom_10_series    : list of floats
#    rs_ratio_21_series  : list of floats
#    rs_mom_21_series    : list of floats
# ============================================================

import io
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

warnings.filterwarnings("ignore")


# ── Screener config ───────────────────────────────────────────
SCREENER_CONFIG = {
    "rs_ratio": {
        "chart_type":   "rs_ratio",
        "summary_cols": ["Signal", "RS_Ratio_10", "RS_Mom_10", "Quadrant_10",
                         "RS_Ratio_21", "RS_Mom_21", "Quadrant_21"],
        "display_name": "RS Ratio (JdK)",
    },
    "macd": {
        "chart_type":      "macd",
        "summary_cols":    ["Signal", "RSI", "MACD", "Signal_Line", "Histogram"],
        "indicator_label": "MACD",
        "display_name":    "MACD Screener",
    },
    "low_vol": {
        "chart_type":      "generic",
        "summary_cols":    ["Signal", "RSI", "ADX", "BB_Width"],
        "indicator_label": "RSI",
        "display_name":    "Low Volatility Screener",
    },
    "oel_oeh": {
        "chart_type":      "generic",
        "summary_cols":    ["Signal", "Pattern", "Pivot", "Strength"],
        "indicator_label": "Pivot",
        "display_name":    "OEL / OEH Pivot Screener",
    },
    "_default": {
        "chart_type":      "generic",
        "summary_cols":    ["Signal", "RSI", "ADX", "MACD"],
        "indicator_label": "RSI",
        "display_name":    "Screener",
    },
}

# ── Colours ───────────────────────────────────────────────────
BG_COLOR      = "#0D1117"
PANEL_COLOR   = "#161B22"
TEXT_COLOR    = "#E6EDF3"
MUTED_COLOR   = "#8B949E"
GRID_COLOR    = "#21262D"
UP_COLOR      = "#3FB950"
DOWN_COLOR    = "#F85149"
LINE_10       = "#58A6FF"
LINE_21       = "#FF7B72"

SIGNAL_COLORS = {
    "STRONG": "#00C853",
    "WATCH":  "#FFD600",
    "SKIP":   "#FF1744",
    "N/A":    "#9E9E9E",
}
QUADRANT_COLORS = {
    "Leading":   "#00C853",
    "Improving": "#64B5F6",
    "Weakening": "#FFA726",
    "Lagging":   "#EF5350",
    "N/A":       "#9E9E9E",
}
QUADRANT_DESC = {
    "Leading":   "RS > 100 & Mom > 100  —  strong & accelerating",
    "Improving": "RS < 100 & Mom > 100  —  weak but turning (early rotation signal)",
    "Weakening": "RS > 100 & Mom < 100  —  strong but fading",
    "Lagging":   "RS < 100 & Mom < 100  —  weak & getting worse",
}


# ── Style helpers ─────────────────────────────────────────────

def _style_fig(fig):
    fig.patch.set_facecolor(BG_COLOR)

def _style_ax(ax, title=None, ylabel=None):
    ax.set_facecolor(PANEL_COLOR)
    ax.tick_params(colors=MUTED_COLOR, labelsize=7)
    ax.spines[:].set_color(GRID_COLOR)
    ax.grid(color=GRID_COLOR, linewidth=0.5, alpha=0.7)
    if title:
        ax.set_title(title, color=TEXT_COLOR, fontsize=8, pad=4)
    if ylabel:
        ax.set_ylabel(ylabel, color=MUTED_COLOR, fontsize=7)


# ── Indicator helpers (for generic screeners) ─────────────────

def _ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def _calc_macd(close):
    ema12  = _ema(close, 12)
    ema26  = _ema(close, 26)
    macd   = ema12 - ema26
    signal = _ema(macd, 9)
    hist   = macd - signal
    return macd, signal, hist

def _calc_rsi(close, period=14):
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ── Candlestick chart ─────────────────────────────────────────

def _draw_candlestick(ax, chart_data: list, title: str = ""):
    """
    Draw candlestick from chart_data list of dicts
    {date, open, high, low, close, volume} — already in the scan result.
    """
    _style_ax(ax, title=title)

    if not chart_data or len(chart_data) < 5:
        ax.text(0.5, 0.5, "Insufficient data", transform=ax.transAxes,
                ha="center", va="center", color=MUTED_COLOR, fontsize=9)
        return

    opens  = [r["open"]  for r in chart_data]
    highs  = [r["high"]  for r in chart_data]
    lows   = [r["low"]   for r in chart_data]
    closes = [r["close"] for r in chart_data]
    dates  = [r["date"]  for r in chart_data]
    xs     = np.arange(len(chart_data))
    width  = 0.6

    for i in range(len(chart_data)):
        color = UP_COLOR if closes[i] >= opens[i] else DOWN_COLOR
        ax.plot([xs[i], xs[i]], [lows[i], highs[i]],
                color=color, linewidth=0.8, zorder=2)
        body_bottom = min(opens[i], closes[i])
        body_height = abs(closes[i] - opens[i]) or highs[i] * 0.001
        ax.add_patch(plt.Rectangle(
            (xs[i] - width / 2, body_bottom), width, body_height,
            color=color, zorder=3
        ))

    # X-axis: show date labels every ~10 bars
    step = max(1, len(chart_data) // 6)
    ax.set_xticks(xs[::step])
    ax.set_xticklabels(
        [dates[i][:10] for i in range(0, len(chart_data), step)],
        rotation=30, ha="right", fontsize=6, color=MUTED_COLOR
    )
    ax.set_xlim(-1, len(chart_data))
    ax.tick_params(axis="y", labelsize=7, colors=MUTED_COLOR)

    # Last price line
    last = closes[-1]
    ax.axhline(last, color=MUTED_COLOR, linewidth=0.5, linestyle="--", alpha=0.5)
    ax.text(len(chart_data) - 0.5, last, f"  {last:.1f}",
            color=TEXT_COLOR, fontsize=7, va="center")
    ax.set_ylabel("Price", color=MUTED_COLOR, fontsize=7)


# ── RS charts ─────────────────────────────────────────────────

def _draw_rs_line(ax, series_10: list, series_21: list, title: str,
                  ylabel: str, last_r10=None, last_r21=None,
                  q10: str = None, q21: str = None):
    """
    Generic RS line chart used for both Ratio and Momentum panels.
    series_10 / series_21 are plain lists of floats from the scan result.
    """
    _style_ax(ax, title=title, ylabel=ylabel)

    xs_10 = np.arange(len(series_10)) if series_10 else []
    xs_21 = np.arange(len(series_21)) if series_21 else []

    if not series_10 and not series_21:
        ax.text(0.5, 0.5, "No data", transform=ax.transAxes,
                ha="center", color=MUTED_COLOR, fontsize=9)
        return

    if series_10:
        ax.plot(xs_10, series_10, color=LINE_10, linewidth=1.2,
                label="EMA-10", zorder=3)
        ax.fill_between(xs_10, series_10, 100,
                        where=[v >= 100 for v in series_10],
                        alpha=0.08, color=UP_COLOR)
        ax.fill_between(xs_10, series_10, 100,
                        where=[v < 100 for v in series_10],
                        alpha=0.08, color=DOWN_COLOR)

    if series_21:
        ax.plot(xs_21, series_21, color=LINE_21, linewidth=1.2,
                label="EMA-21", zorder=3)

    # 100 reference
    ax.axhline(100, color=MUTED_COLOR, linewidth=0.8, linestyle="--", alpha=0.6)
    n = max(len(series_10), len(series_21), 1)
    ax.text(n - 1, 100.5, "100", color=MUTED_COLOR, fontsize=6, va="bottom")

    # Right-side annotations
    if last_r10 is not None:
        ax.text(1.01, 0.85, f"10: {last_r10:.1f}", transform=ax.transAxes,
                color=LINE_10, fontsize=7, va="center")
    if q10:
        ax.text(1.01, 0.73, q10, transform=ax.transAxes,
                color=QUADRANT_COLORS.get(q10, MUTED_COLOR),
                fontsize=6, va="center", style="italic")
    if last_r21 is not None:
        ax.text(1.01, 0.55, f"21: {last_r21:.1f}", transform=ax.transAxes,
                color=LINE_21, fontsize=7, va="center")
    if q21:
        ax.text(1.01, 0.43, q21, transform=ax.transAxes,
                color=QUADRANT_COLORS.get(q21, MUTED_COLOR),
                fontsize=6, va="center", style="italic")

    ax.legend(fontsize=6, loc="upper left",
              facecolor=PANEL_COLOR, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)
    ax.tick_params(axis="x", labelbottom=False)
    ax.set_xlim(-1, n)


# ── Generic indicator chart ───────────────────────────────────

def _draw_indicator(ax, chart_data: list, indicator_label: str, title: str = ""):
    """
    Draw indicator (MACD / RSI) computed from chart_data close prices.
    chart_data is the same list already in the result — no new downloads.
    """
    _style_ax(ax, title=title)

    if not chart_data or len(chart_data) < 20:
        ax.text(0.5, 0.5, "Insufficient data", transform=ax.transAxes,
                ha="center", color=MUTED_COLOR)
        return

    close = pd.Series([r["close"] for r in chart_data])
    xs    = np.arange(len(close))
    label = indicator_label.upper()

    if "MACD" in label:
        macd, signal, hist = _calc_macd(close)
        colors = [UP_COLOR if v >= 0 else DOWN_COLOR for v in hist.values]
        ax.bar(xs, hist.values, color=colors, alpha=0.6, width=0.8, zorder=2)
        ax.plot(xs, macd.values,   color=LINE_10, linewidth=1.0, label="MACD",   zorder=3)
        ax.plot(xs, signal.values, color=LINE_21, linewidth=1.0, label="Signal", zorder=3)
        ax.axhline(0, color=MUTED_COLOR, linewidth=0.6, linestyle="--")
        ax.legend(fontsize=6, loc="upper left",
                  facecolor=PANEL_COLOR, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)
        ax.set_ylabel("MACD", color=MUTED_COLOR, fontsize=7)

    elif "RSI" in label:
        rsi = _calc_rsi(close)
        ax.plot(xs, rsi.values, color=LINE_10, linewidth=1.2, zorder=3)
        ax.axhline(70, color=DOWN_COLOR,  linewidth=0.7, linestyle="--", alpha=0.7)
        ax.axhline(30, color=UP_COLOR,    linewidth=0.7, linestyle="--", alpha=0.7)
        ax.axhline(50, color=MUTED_COLOR, linewidth=0.5, linestyle="--", alpha=0.4)
        ax.fill_between(xs, rsi.values, 70, where=(rsi.values >= 70),
                        alpha=0.15, color=DOWN_COLOR)
        ax.fill_between(xs, rsi.values, 30, where=(rsi.values <= 30),
                        alpha=0.15, color=UP_COLOR)
        ax.set_ylim(0, 100)
        ax.set_ylabel("RSI", color=MUTED_COLOR, fontsize=7)
        last = rsi.dropna().iloc[-1] if not rsi.dropna().empty else None
        if last:
            ax.text(0.98, 0.85, f"RSI: {last:.1f}", transform=ax.transAxes,
                    color=LINE_10, fontsize=7, ha="right")
    else:
        ax.plot(xs, close.values, color=LINE_10, linewidth=1.0)
        ax.set_ylabel(indicator_label, color=MUTED_COLOR, fontsize=7)

    ax.tick_params(axis="x", labelbottom=False)


# ── Stock header ──────────────────────────────────────────────

def _draw_header(fig, stock: dict, cfg: dict, run_date: str):
    """Name, signal, price, quadrant pills, quadrant legend."""
    name   = stock.get("symbol", stock.get("ticker", ""))
    ticker = stock.get("ticker", "")
    signal = stock.get("signal", "N/A")
    price  = stock.get("price", "")
    change = stock.get("change", "")

    sig_color    = SIGNAL_COLORS.get(signal, MUTED_COLOR)
    change_color = UP_COLOR if (change or 0) >= 0 else DOWN_COLOR
    change_str   = f"{'+' if (change or 0) >= 0 else ''}{change:.2f}%" if change else ""

    # Row 1: name + signal + price
    fig.text(0.03, 0.975, f"{name}  ({ticker})",
             color=TEXT_COLOR, fontsize=12, fontweight="bold", va="top")
    fig.text(0.50, 0.975, f"Signal: {signal}",
             color=sig_color,  fontsize=10, fontweight="bold", va="top")
    fig.text(0.65, 0.975, f"₹{price:,.2f}  {change_str}" if price else "",
             color=change_color, fontsize=9, va="top")
    fig.text(0.85, 0.975, f"As of {run_date}",
             color=MUTED_COLOR, fontsize=8, va="top")

    if cfg["chart_type"] == "rs_ratio":
        q10 = stock.get("Quadrant_10", "N/A")
        q21 = stock.get("Quadrant_21", "N/A")
        r10 = stock.get("RS_Ratio_10")
        m10 = stock.get("RS_Mom_10")
        r21 = stock.get("RS_Ratio_21")
        m21 = stock.get("RS_Mom_21")

        # Row 2: quadrant pills
        fig.text(0.03, 0.945, "10-EMA:", color=MUTED_COLOR, fontsize=8, va="top")
        fig.text(0.10, 0.945, q10,
                 color=QUADRANT_COLORS.get(q10, MUTED_COLOR),
                 fontsize=8, fontweight="bold", va="top")
        if r10 is not None:
            fig.text(0.20, 0.945, f"Ratio {r10:.1f}  |  Mom {m10:.1f}",
                     color=MUTED_COLOR, fontsize=7, va="top")

        fig.text(0.50, 0.945, "21-EMA:", color=MUTED_COLOR, fontsize=8, va="top")
        fig.text(0.57, 0.945, q21,
                 color=QUADRANT_COLORS.get(q21, MUTED_COLOR),
                 fontsize=8, fontweight="bold", va="top")
        if r21 is not None:
            fig.text(0.67, 0.945, f"Ratio {r21:.1f}  |  Mom {m21:.1f}",
                     color=MUTED_COLOR, fontsize=7, va="top")

        # Row 3: quadrant legend
        fig.text(0.03, 0.915, "Guide:", color=MUTED_COLOR, fontsize=7, va="top")
        offsets = [0.09, 0.33, 0.57, 0.78]
        for (q, desc), x in zip(QUADRANT_DESC.items(), offsets):
            fig.text(x, 0.915, f"{q} — ", color=QUADRANT_COLORS[q],
                     fontsize=6, fontweight="bold", va="top")
            fig.text(x + 0.075, 0.915, desc,
                     color=MUTED_COLOR, fontsize=6, va="top")

    else:
        # Generic: show key extras as a flat row
        x_pos = 0.03
        for key, val in stock.items():
            if key in ("symbol", "ticker", "signal", "price", "change",
                       "chart", "reasons"):
                continue
            if val is None or isinstance(val, list):
                continue
            label   = key.replace("_", " ")
            val_str = f"{val:.2f}" if isinstance(val, float) else str(val)
            fig.text(x_pos, 0.945, f"{label}: ", color=MUTED_COLOR, fontsize=7, va="top")
            fig.text(x_pos + 0.08, 0.945, val_str,
                     color=TEXT_COLOR, fontsize=7, fontweight="bold", va="top")
            x_pos += 0.18
            if x_pos > 0.85:
                break


# ── RS Ratio page ─────────────────────────────────────────────

def _build_rs_ratio_page(pdf: PdfPages, stock: dict, cfg: dict, run_date: str):
    """
    Layout:
      Left  col : candlestick (full height)
      Right col : RS Ratio chart (top) + RS Momentum chart (bottom)
      Header    : name, signal, quadrant pills, quadrant legend
    """
    chart_data = stock.get("chart", [])

    r10_series = stock.get("rs_ratio_10_series", [])
    m10_series = stock.get("rs_mom_10_series",   [])
    r21_series = stock.get("rs_ratio_21_series", [])
    m21_series = stock.get("rs_mom_21_series",   [])

    q10 = stock.get("Quadrant_10", "N/A")
    q21 = stock.get("Quadrant_21", "N/A")
    r10 = stock.get("RS_Ratio_10")
    m10 = stock.get("RS_Mom_10")
    r21 = stock.get("RS_Ratio_21")
    m21 = stock.get("RS_Mom_21")

    fig = plt.figure(figsize=(14, 9))
    _style_fig(fig)

    chart_top    = 0.88
    chart_bottom = 0.04
    mid          = (chart_top + chart_bottom) / 2

    ax_candle = fig.add_axes([0.03, chart_bottom, 0.52, chart_top - chart_bottom])
    ax_ratio  = fig.add_axes([0.61, mid + 0.01,   0.32, (chart_top - chart_bottom) / 2 - 0.015])
    ax_mom    = fig.add_axes([0.61, chart_bottom,  0.32, mid - chart_bottom - 0.01])

    name   = stock.get("symbol", stock.get("ticker", ""))
    ticker = stock.get("ticker", "")

    _draw_candlestick(ax_candle, chart_data,
                      title=f"{ticker}  —  Daily Candles (60 days)")
    _draw_rs_line(ax_ratio, r10_series, r21_series,
                  title="RS Ratio", ylabel="RS Ratio",
                  last_r10=r10, last_r21=r21)
    _draw_rs_line(ax_mom, m10_series, m21_series,
                  title="RS Momentum", ylabel="RS Momentum",
                  last_r10=m10, last_r21=m21, q10=q10, q21=q21)

    _draw_header(fig, stock, cfg, run_date)

    pdf.savefig(fig, facecolor=BG_COLOR)
    plt.close(fig)


# ── Generic page ──────────────────────────────────────────────

def _build_generic_page(pdf: PdfPages, stock: dict, cfg: dict, run_date: str):
    """
    Layout:
      Top    : candlestick
      Bottom : indicator (MACD / RSI computed from chart_data close)
      Header : name, signal, key values
    """
    chart_data       = stock.get("chart", [])
    indicator_label  = cfg.get("indicator_label", "RSI")
    ticker           = stock.get("ticker", "")

    fig = plt.figure(figsize=(14, 9))
    _style_fig(fig)

    chart_top    = 0.88
    chart_bottom = 0.04
    split        = 0.35

    ax_candle = fig.add_axes([0.05, chart_bottom + split, 0.90,
                               chart_top - chart_bottom - split - 0.01])
    ax_ind    = fig.add_axes([0.05, chart_bottom,          0.90, split - 0.02])

    _draw_candlestick(ax_candle, chart_data,
                      title=f"{ticker}  —  Daily Candles (60 days)")
    _draw_indicator(ax_ind, chart_data, indicator_label,
                    title=indicator_label)

    _draw_header(fig, stock, cfg, run_date)

    pdf.savefig(fig, facecolor=BG_COLOR)
    plt.close(fig)


# ── Summary page ──────────────────────────────────────────────

def _build_summary_page(pdf: PdfPages, stocks: list, cfg: dict,
                         screener_type: str, filters_applied: dict, run_date: str):
    display_name = cfg.get("display_name", screener_type)
    summary_cols = cfg.get("summary_cols", ["Signal"])

    rows = []
    for s in stocks:
        row = {"Stock": s.get("symbol", s.get("ticker", "")),
               "Ticker": s.get("ticker", ""),
               "Signal": s.get("signal", "N/A")}
        for col in summary_cols:
            if col == "Signal":
                continue
            val = s.get(col)
            row[col] = f"{val:.2f}" if isinstance(val, float) else (str(val) if val is not None else "—")
        rows.append(row)

    display_cols = ["Stock", "Ticker", "Signal"] + [c for c in summary_cols if c != "Signal"]
    n_rows = len(rows)
    n_cols = len(display_cols)

    fig_h = max(8, min(18, 3 + n_rows * 0.32))
    fig   = plt.figure(figsize=(14, fig_h))
    _style_fig(fig)

    # Title block
    fig.text(0.05, 0.97, f"OptionLab  —  {display_name}",
             color=TEXT_COLOR, fontsize=14, fontweight="bold", va="top")
    fig.text(0.05, 0.93, f"Run date: {run_date}   |   Stocks: {n_rows}",
             color=MUTED_COLOR, fontsize=9, va="top")

    if filters_applied:
        fstr = "  |  ".join(f"{k}: {v}" for k, v in filters_applied.items() if v)
        if fstr:
            fig.text(0.05, 0.90, f"Filters: {fstr}", color=MUTED_COLOR, fontsize=8, va="top")

    # Signal breakdown
    sig_counts = {}
    for s in stocks:
        sig = s.get("signal", "N/A")
        sig_counts[sig] = sig_counts.get(sig, 0) + 1
    x_off = 0.05
    fig.text(x_off, 0.87, "Breakdown: ", color=MUTED_COLOR, fontsize=8, va="top")
    x_off += 0.09
    for sig, cnt in sig_counts.items():
        fig.text(x_off, 0.87, f"{sig}: {cnt}   ",
                 color=SIGNAL_COLORS.get(sig, MUTED_COLOR),
                 fontsize=8, fontweight="bold", va="top")
        x_off += 0.11

    # Table
    ax = fig.add_axes([0.03, 0.04, 0.94, 0.80])
    ax.set_facecolor(BG_COLOR)
    ax.axis("off")

    cell_text = [[row.get(c, "—") for c in display_cols] for row in rows]

    cell_colors = []
    for row in rows:
        rc = []
        for c in display_cols:
            if c == "Signal":
                base = SIGNAL_COLORS.get(row.get("Signal", "N/A"), MUTED_COLOR)
                rc.append(base + "33")
            elif "Quadrant" in c:
                base = QUADRANT_COLORS.get(row.get(c, "N/A"), MUTED_COLOR)
                rc.append(base + "22")
            else:
                rc.append(PANEL_COLOR)
        cell_colors.append(rc)

    tbl = ax.table(
        cellText=cell_text,
        colLabels=display_cols,
        cellColours=cell_colors,
        colColours=[GRID_COLOR] * n_cols,
        loc="upper center",
        cellLoc="center",
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.scale(1, 1.4)

    for (r, c), cell in tbl.get_celld().items():
        cell.set_edgecolor(GRID_COLOR)
        if r == 0:
            cell.set_text_props(color=TEXT_COLOR, fontweight="bold")
        else:
            row_data = rows[r - 1]
            col_name = display_cols[c]
            if col_name == "Signal":
                cell.set_text_props(
                    color=SIGNAL_COLORS.get(row_data.get("Signal", "N/A"), MUTED_COLOR),
                    fontweight="bold")
            elif "Quadrant" in col_name:
                cell.set_text_props(
                    color=QUADRANT_COLORS.get(row_data.get(col_name, "N/A"), MUTED_COLOR))
            else:
                cell.set_text_props(color=TEXT_COLOR)

    pdf.savefig(fig, facecolor=BG_COLOR)
    plt.close(fig)


# ── Main entry point ──────────────────────────────────────────

def generate_pdf(screener_type: str, stocks: list,
                 filters_applied: dict = None) -> bytes:
    """
    Generate screener PDF from scan results.

    Args:
        screener_type   : filename stem e.g. "rs_ratio_screener" or "macd_screener"
                          Matched against SCREENER_CONFIG keys by prefix check.
        stocks          : list of result dicts from /scan endpoint
                          (each row as server.py builds it — chart, extras already in)
        filters_applied : dict of active dashboard filters for annotation

    Returns:
        PDF as bytes
    """
    if not stocks:
        raise ValueError("No stocks provided")

    # Match screener type — strip _screener suffix, match against config keys
    stype = screener_type.replace("_screener.py", "").replace(".py", "")
    cfg   = next(
        (v for k, v in SCREENER_CONFIG.items() if stype.startswith(k)),
        SCREENER_CONFIG["_default"]
    )

    run_date        = datetime.now().strftime("%d %b %Y")
    filters_applied = filters_applied or {}

    buf = io.BytesIO()

    with PdfPages(buf) as pdf:

        # Summary page — only for multiple stocks
        if len(stocks) > 1:
            print(f"  [PDF] Summary page ({len(stocks)} stocks)...")
            _build_summary_page(pdf, stocks, cfg, screener_type,
                                 filters_applied, run_date)

        total = len(stocks)
        for i, stock in enumerate(stocks):
            ticker = stock.get("ticker", f"Stock {i+1}")
            print(f"  [PDF] {i+1}/{total}: {ticker}")
            try:
                if cfg["chart_type"] == "rs_ratio":
                    _build_rs_ratio_page(pdf, stock, cfg, run_date)
                else:
                    _build_generic_page(pdf, stock, cfg, run_date)
            except Exception as e:
                print(f"  [PDF] WARNING: {ticker} failed: {e}")
                fig = plt.figure(figsize=(14, 9))
                _style_fig(fig)
                fig.text(0.5, 0.5, f"Error rendering {ticker}:\n{e}",
                         color=DOWN_COLOR, ha="center", va="center", fontsize=12)
                pdf.savefig(fig, facecolor=BG_COLOR)
                plt.close(fig)

        d = pdf.infodict()
        d["Title"]   = f"OptionLab — {cfg.get('display_name', screener_type)}"
        d["Author"]  = "OptionLab"
        d["Subject"] = f"Screener results — {run_date}"

    buf.seek(0)
    print(f"  [PDF] Done. {len(buf.getvalue()) / 1024:.1f} KB")
    return buf.getvalue()
