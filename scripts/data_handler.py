import datetime
import pandas as pd

from pandas.tseries.offsets import BDay
from config import logger, EASTERN_TZ, OUTPUT_DIR

def get_screening_date_now() -> datetime.date:

    now = datetime.datetime.now(EASTERN_TZ)

    if now.time() > datetime.time(20, 0):
        return (now + BDay(1)).date()
    
    return now.date()

def get_data_for_date(df: pd.DataFrame, target_date: datetime.date) -> pd.DataFrame:
    return df[df.index.date == target_date]

def find_previous_16h_open(df: pd.DataFrame, screening_date: datetime.date) -> float | None:

    """
    Search for the most recent available 16h bar before screening_date (max 7 days).
    """

    for delta in range(1, 8):

        day = screening_date - datetime.timedelta(days=delta)
        data = get_data_for_date(df, day)

        if not data.empty:
            bar_16 = data.between_time("16:00", "16:59")

            if not bar_16.empty:
                open_val = bar_16.iloc[-1]["Open"]
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

def find_previous_day_data(df: pd.DataFrame, start_date: datetime.date, max_lookback_days: int = 7) -> pd.DataFrame | None:

    day_cursor = start_date - datetime.timedelta(days=1)

    for _ in range(max_lookback_days):
        data = get_data_for_date(df, day_cursor)

        if not data.empty:
            return data
        
        day_cursor -= datetime.timedelta(days=1)

    return None
