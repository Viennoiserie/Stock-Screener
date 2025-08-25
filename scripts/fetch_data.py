from __future__ import annotations

import pytz
import random
import asyncio
import pandas as pd
import datetime as dt

from ib_insync import IB, Stock, util
from config import EASTERN_TZ, logger
from typing import Dict, List, Optional, Tuple

# region : Variables

__all__ = ["fetch_all_data",
           "MAX_CONCURRENCY",
           "REQUEST_DELAY_SEC",
           "MAX_RETRIES",
           "BACKOFF_BASE"]

FAST = {
    "MAX_RETRIES": 1,
    "MAX_CONCURRENCY": 96,
    "BACKOFF_BASE": 0.12,
    "REQUEST_DELAY_SEC": 0.0,
}

SLOW = {
    "MAX_RETRIES": 2,
    "MAX_CONCURRENCY": 3,
    "BACKOFF_BASE": 2.0,
    "REQUEST_DELAY_SEC": 0.6,
}

MAX_RETRIES: int = FAST["MAX_RETRIES"]
MAX_CONCURRENCY: int = FAST["MAX_CONCURRENCY"]

BACKOFF_BASE: float = FAST["BACKOFF_BASE"]
REQUEST_DELAY_SEC: float = FAST["REQUEST_DELAY_SEC"]

PACING_ERROR_CODES = {354, 420}
RETRYABLE_ERROR_CODES = {162, 321, 354, 420}

# Cache contrats pour éviter re-qualifications
_CONTRACT_CACHE: Dict[str, Stock] = {}

# endregion

# region : Helper Finctions

def _today_eastern_date() -> dt.date:
    return dt.datetime.now(EASTERN_TZ).date()

def _age_in_days(screening_date: dt.date) -> int:
    return abs((_today_eastern_date() - screening_date).days)


def _pick_profile(screening_date: dt.date):

    """
    ≤ 6 jours  -> FAST (ultra-rapide, tel quel)
    > 6 jours  -> SLOW (backoff exponentiel + petit délai)
    """

    age = _age_in_days(screening_date)

    if age <= 6:
        prof, label = FAST, "FAST"

    else:
        prof, label = SLOW, "SLOW"

    global MAX_RETRIES, MAX_CONCURRENCY, BACKOFF_BASE, REQUEST_DELAY_SEC

    MAX_RETRIES = prof["MAX_RETRIES"]
    MAX_CONCURRENCY = prof["MAX_CONCURRENCY"]

    BACKOFF_BASE = prof["BACKOFF_BASE"]
    REQUEST_DELAY_SEC = prof["REQUEST_DELAY_SEC"]

    logger.info(f"[FETCH_PROFILE] {label} — age={age}d "
                f"(MAX_CONCURRENCY={MAX_CONCURRENCY}, "
                f"RETRIES={MAX_RETRIES}, "
                f"DELAY={REQUEST_DELAY_SEC}s, "
                f"BACKOFF_BASE={BACKOFF_BASE})")
    
    return prof, label

def _compute_end_datetime_str(screening_date: dt.date) -> str:

    """
    Fenêtre IB: fin = lendemain 11:00 US/Eastern → UTC.
    Format IB: 'YYYYMMDD HH:MM:SS' (UTC), pas de suffixe.
    """

    next_day_11 = dt.datetime.combine(screening_date + dt.timedelta(days=1), dt.time(11, 0))
    utc_end = EASTERN_TZ.localize(next_day_11).astimezone(pytz.utc)
    return utc_end.strftime("%Y%m%d %H:%M:%S")


def _needed_duration_days(_: dt.date) -> int:
    return 3

def _parse_ib_error_code(e: Exception) -> Optional[int]:

    msg = str(e).lower()

    for code in RETRYABLE_ERROR_CODES:

        if f"error {code}" in msg or f"code {code}" in msg:
            return code
        
    if "no data" in msg or ("hmds" in msg and "no" in msg):
        return 162
    
    return None


def _bars_to_df(bars) -> pd.DataFrame:

    if not bars:
        return pd.DataFrame()
    
    df = util.df(bars)

    if df.empty:
        return pd.DataFrame()
    
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    idx = df.index

    if idx.tz is None:
        df.index = idx.tz_localize("UTC").tz_convert(EASTERN_TZ)

    else:
        df.index = idx.tz_convert(EASTERN_TZ)

    return df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"})

async def _qualify_contract(ib: IB, ticker: str) -> Optional[Stock]:

    try:

        if ticker in _CONTRACT_CACHE:
            return _CONTRACT_CACHE[ticker]
        
        c = Stock(ticker, "SMART", "USD")
        await ib.qualifyContractsAsync(c)

        _CONTRACT_CACHE[ticker] = c
        return c
    
    except Exception as e:
        logger.error(f"[QUALIFY] {ticker}: {e}")
        return None

# endregion

# region : Fetch Functions

async def _fetch_one_fast(ib: IB,
                          contract: Stock,
                          end_time_str: str,
                          *,
                          screening_date: dt.date,
                          max_retries: int,
                          backoff_base: float,
                          delay_after: float) -> Tuple[str, pd.DataFrame]:
    
    """
    FAST (ultra-rapide tel quel) :
      - 3D, 1h, TRADES, useRTH=False
      - 1 retry pacing only, backoff court + léger jitter
      - pas de délai inter-requête
    """

    symbol = contract.symbol
    duration_days = _needed_duration_days(screening_date)

    attempt = 0

    while True:

        try:
            bars = await ib.reqHistoricalDataAsync(contract,
                                                   endDateTime=end_time_str,
                                                   durationStr=f"{duration_days} D",
                                                   barSizeSetting="1 hour",
                                                   whatToShow="TRADES",
                                                   useRTH=False,
                                                   formatDate=1)
            
            return symbol, _bars_to_df(bars)

        except Exception as e:

            attempt += 1
            code = _parse_ib_error_code(e)

            if code == 162:
                logger.debug(f"[NO_DATA] {symbol}: {e}")
                return symbol, pd.DataFrame()

            if code in PACING_ERROR_CODES and attempt <= max_retries:

                wait = backoff_base + random.uniform(0, backoff_base)
                logger.warning(f"[PACING RETRY {attempt}/{max_retries}] {symbol} — sleep {wait:.2f}s (code {code})")

                await asyncio.sleep(wait)
                continue

            logger.error(f"[FETCH_FAIL] {symbol}: {e}")
            return symbol, pd.DataFrame()

async def _fetch_one_slow(ib: IB,
                          contract: Stock,
                          end_time_str: str,
                          *,
                          screening_date: dt.date,
                          delay_after: float,
                          max_retries: int,
                          backoff_base: float) -> Tuple[str, pd.DataFrame]:
    
    """
    SLOW (ta logique) :
      - 3D, 1h, TRADES, useRTH=False
      - backoff exponentiel BACKOFF_BASE ** (attempt-1)
      - petit délai inter-requête après succès
    """

    symbol = contract.symbol
    attempt = 0

    backoff = 0.0
    duration_days = _needed_duration_days(screening_date)

    while True:

        if backoff > 0:
            await asyncio.sleep(backoff)

        try:
            bars = await ib.reqHistoricalDataAsync(contract,
                                                   endDateTime=end_time_str,
                                                   durationStr=f"{duration_days} D",
                                                   barSizeSetting="1 hour",
                                                   whatToShow="TRADES",
                                                   useRTH=False,
                                                   formatDate=1)
            
            df = _bars_to_df(bars)

            if delay_after > 0:
                await asyncio.sleep(delay_after)

            return symbol, df

        except Exception as e:

            attempt += 1
            err_code = _parse_ib_error_code(e)

            if err_code in RETRYABLE_ERROR_CODES and attempt <= max_retries:

                backoff = backoff_base ** (attempt - 1)

                logger.warning(f"[RETRY {attempt}/{max_retries}] {symbol} (code={err_code}): {e} — backoff {backoff:.2f}s")
                continue

            logger.error(f"[FETCH_FAIL] {symbol}: {e}")
            return symbol, pd.DataFrame()

# endregion

# region : API Functions

async def fetch_all_data(ib: IB, tickers: List[str], screening_date: dt.date) -> Dict[str, pd.DataFrame]:

    """
    Bascule auto :
      - ≤ 6 jours  -> FAST (ultra-rapide tel quel)
      - > 6 jours  -> SLOW (ta logique lente : concurrence faible + backoff expo)
    """

    if not tickers:
        logger.warning("[FETCH] Aucun ticker fourni.")
        return {}

    prof, label = _pick_profile(screening_date)
    end_time_str = _compute_end_datetime_str(screening_date)
    logger.info(f"[FETCH] endDateTime={end_time_str} | tickers={len(tickers)}")

    results: Dict[str, pd.DataFrame] = {}

    if label == "FAST":

        sem = asyncio.Semaphore(min(prof["MAX_CONCURRENCY"], max(1, len(tickers))))

        async def _qualify_and_fetch(ticker: str):

            async with sem:
                c = await _qualify_contract(ib, ticker)

            if c is None:
                results[ticker] = pd.DataFrame()
                return

            async with sem:
                sym, df = await _fetch_one_fast(ib, c, end_time_str,
                                                screening_date=screening_date,
                                                max_retries=prof["MAX_RETRIES"],
                                                backoff_base=prof["BACKOFF_BASE"],
                                                delay_after=prof["REQUEST_DELAY_SEC"])
                
            results[sym] = df

        await asyncio.gather(*[_qualify_and_fetch(t) for t in tickers], return_exceptions=False)

    else:
       
        async def _qualify_one(t: str) -> tuple[str, Optional[Stock]]:

            c = await _qualify_contract(ib, t)
            return t, c

        pairs = await asyncio.gather(*[_qualify_one(t) for t in tickers])
        qualified: Dict[str, Optional[Stock]] = {t: c for t, c in pairs}

        for t, c in pairs:
            if c is None:
                logger.warning(f"[SKIP] {t}: contrat non qualifié")

        sem = asyncio.Semaphore(prof["MAX_CONCURRENCY"])

        async def _worker(ticker: str, contract: Optional[Stock]) -> None:

            if contract is None:
                results[ticker] = pd.DataFrame()
                return

            async with sem:
                sym, df = await _fetch_one_slow(ib, contract, end_time_str,
                                                screening_date=screening_date,
                                                delay_after=prof["REQUEST_DELAY_SEC"],
                                                max_retries=prof["MAX_RETRIES"],
                                                backoff_base=prof["BACKOFF_BASE"])
                
                results[sym] = df

        await asyncio.gather(*[_worker(t, c) for t, c in qualified.items()], return_exceptions=False)

    ok = sum(1 for df in results.values() if not df.empty)
    ko = len(results) - ok

    logger.info(f"[FETCH_DONE] {ok} OK / {ko} KO / total {len(results)}")
    return results

# endregion
