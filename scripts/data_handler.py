import pytz
import datetime
import pandas as pd

from ib_connect import get_ib
from ib_insync import Stock, util
from pandas.tseries.offsets import BDay
from config import logger, EASTERN_TZ, OUTPUT_DIR

def get_screening_date_now() -> datetime.date:

    now = datetime.datetime.now(EASTERN_TZ)

    if now.time() > datetime.time(20, 0):
        return (now + BDay(1)).date()
    
    return now.date()

def fetch_hourly_data(ticker: str, end_date: datetime.date) -> pd.DataFrame:

    """
    Fetch 1-hour historical OHLCV data (7 days back) for a given ticker.
    """

    ib = get_ib()

    # Check API connection
    if ib is None:

        logger.error("IB connection failed. Aborting data fetch.")
        return pd.DataFrame()

    try:
        contract = Stock(ticker, "SMART", "USD")
        ib.qualifyContracts(contract)

        target_datetime = datetime.datetime.combine(end_date, datetime.time(23, 59))
        localized_dt = EASTERN_TZ.localize(target_datetime).astimezone(pytz.utc)
        end_time_str = localized_dt.strftime("%Y%m%d-%H:%M:%S")

        logger.info(f"Requesting data for {ticker} until {end_time_str} (extended hours).")

        bars = ib.reqHistoricalData(contract,
                                    
                                    endDateTime=end_time_str,
                                    durationStr="7 D",

                                    barSizeSetting="1 hour",
                                    whatToShow="TRADES",

                                    useRTH=False,
                                    formatDate=1)

        if not bars:
            logger.warning(f"No data returned for {ticker}")
            return pd.DataFrame()

        df = util.df(bars)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        # Normalize Datetime
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(EASTERN_TZ)

        else:
            df.index = df.index.tz_convert(EASTERN_TZ)

        df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"}, inplace=True)

        # Output for logging
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_DIR / f"{ticker}_raw_data.csv")
        logger.info(f"Data for {ticker} saved to output/{ticker}_raw_data.csv")

        return df

    except Exception as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()

def get_data_for_date(df: pd.DataFrame, target_date: datetime.date) -> pd.DataFrame:
    return df[df.index.date == target_date]

def find_previous_16h_open(df: pd.DataFrame, screening_date: datetime.date) -> float | None:

    day_cursor = screening_date - datetime.timedelta(days=1)

    for _ in range(7):
        data = get_data_for_date(df, day_cursor)

        if not data.empty:
            bar_16 = data.between_time("16:00", "16:00")

            if not bar_16.empty:

                open_val = bar_16.iloc[-1]["Open"]
                logger.info(f"Found 16h open for {day_cursor}: {open_val}")

                return open_val
            
        day_cursor -= datetime.timedelta(days=1)

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
