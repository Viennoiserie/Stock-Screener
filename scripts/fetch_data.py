import asyncio
import pandas as pd
import datetime, pytz

from ib_insync import Stock, util
from config import EASTERN_TZ, logger

async def fetch_all_data(ib, tickers: list[str], screening_date: datetime.date) -> dict[str, pd.DataFrame]:

    contracts = [Stock(t, "SMART", "USD") for t in tickers]

    try:
        await ib.qualifyContractsAsync(*contracts)

    except Exception as e:
        logger.error(f"Contract qualification failed: {e}")
        return {}

    target_dt = datetime.datetime.combine(screening_date, datetime.time(23, 59))
    utc_end = EASTERN_TZ.localize(target_dt).astimezone(pytz.utc)
    end_str = utc_end.strftime("%Y%m%d-%H:%M:%S")

    tasks = [ib.reqHistoricalDataAsync(contract,
                                       endDateTime=end_str,

                                       durationStr="7 D",
                                       barSizeSetting="1 hour",

                                       whatToShow="TRADES",
                                       useRTH=False,

                                       formatDate=1) for contract in contracts]

    bars_list = await asyncio.gather(*tasks, return_exceptions=True)

    result = {}

    for contract, bars in zip(contracts, bars_list):

        if isinstance(bars, Exception):
            logger.error(f"Data fetch failed for {contract.symbol}: {bars}")
            result[contract.symbol] = pd.DataFrame()
            continue

        if not bars:
            logger.warning(f"No data for {contract.symbol}")
            result[contract.symbol] = pd.DataFrame()
            continue

        df = util.df(bars)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)

        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(EASTERN_TZ)

        else:
            df.index = df.index.tz_convert(EASTERN_TZ)

        df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"}, inplace=True)
        result[contract.symbol] = df

    return result
