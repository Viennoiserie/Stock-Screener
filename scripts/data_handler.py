import pandas as pd
import datetime as dt

from pandas.tseries.offsets import BDay
from config import logger, EASTERN_TZ, OUTPUT_DIR

# region : Helper Functions

def _day_bounds_eastern(target_date: dt.date):

    """Délimite [YYYY-mm-dd 00:00, +1j) en US/Eastern tz-aware."""

    start = EASTERN_TZ.localize(dt.datetime.combine(target_date, dt.time(0, 0)))
    end = start + pd.Timedelta(days=1)

    return start, end

def _make_hour_bar(win: pd.DataFrame) -> dict | None:

    if win is None or win.empty:
        return None
    
    for c in ("Open", "High", "Low", "Close"):

        if c not in win:
            return None
        
    o = win["Open"].iloc[0]
    h = win["High"].max()
    l = win["Low"].min()
    c = win["Close"].iloc[-1]

    if pd.isna(o) or pd.isna(h) or pd.isna(l) or pd.isna(c):
        return None
    
    if "Volume" in win:
        vol = win["Volume"].sum()

        if pd.isna(vol) or vol <= 0:
            return None
        
    return {"Open": float(o), "High": float(h), "Low": float(l), "Close": float(c)}

def _slice_day(df: pd.DataFrame, target_date: dt.date) -> pd.DataFrame:

    if df is None or df.empty:
        return pd.DataFrame()
    
    start, end = _day_bounds_eastern(target_date)
    idx = df.index.tz_convert(EASTERN_TZ) if df.index.tz is not None else df.index.tz_localize(EASTERN_TZ)

    return df.loc[(idx >= start) & (idx < end)].copy()

def _hour_window(df: pd.DataFrame, target_date: dt.date, hour: int) -> pd.DataFrame:

    day = _slice_day(df, target_date)

    if day.empty:
        return day
    
    start = EASTERN_TZ.localize(dt.datetime.combine(target_date, dt.time(hour, 0)))
    end = start + pd.Timedelta(hours=1)

    return day.loc[(day.index >= start) & (day.index < end)]

# endregion

# region : API Functions

def get_screening_date_now() -> dt.date:

    now = dt.datetime.now(EASTERN_TZ)

    if now.time() > dt.time(20, 0):
        return (now + BDay(1)).date()
    
    return now.date()

def get_data_for_date(df: pd.DataFrame, target_date: dt.date) -> pd.DataFrame:
    """Slice correct en US/Eastern (évite les pièges de .date)."""
    return _slice_day(df, target_date)

def find_previous_16h_open(df: pd.DataFrame, screening_date: dt.date) -> float | None:

    """
    Cherche l'open de la barre 16h (fenêtre 16:00–16:59 ET) d'un jour ouvert < screening_date (max 7 jours).
    On agrège les minutes -> OHLC 1h pour éviter les 'N/A'.
    """

    for delta in range(1, 8):

        day = screening_date - dt.timedelta(days=delta)
        win = _hour_window(df, day, 16)
        bar = _make_hour_bar(win)

        if bar is not None:

            open_val = bar["Open"]
            logger.info(f"Found 16h open for {day}: {open_val}")
            return open_val
        
    logger.warning(f"No 16h bar found in past 7 days before {screening_date}")
    return None

def save_screener_results(results: list[tuple], filename: str = "screener_results.txt") -> None:

    path = OUTPUT_DIR / filename
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        f.write("Serial\tTickerNo\tTicker\tOpen16hDay-1\n")

        for serial, ticker_no, ticker, open_val in sorted(results, key=lambda x: x[0]):
            f.write(f"{serial}\t{ticker_no}\t{ticker}\t{open_val}\n")

    logger.info(f"Results saved to {path}")

def find_previous_day_data(df: pd.DataFrame, start_date: dt.date, max_lookback_days: int = 7) -> pd.DataFrame | None:

    day_cursor = start_date - dt.timedelta(days=1)

    for _ in range(max_lookback_days):
        data = _slice_day(df, day_cursor)

        if not data.empty:
            return data
        
        day_cursor -= dt.timedelta(days=1)

    return None

# endregion
