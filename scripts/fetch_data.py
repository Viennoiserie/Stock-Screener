from __future__ import annotations

import pytz
import asyncio
import pandas as pd
import datetime as dt

from config import EASTERN_TZ, logger
from ib_insync import IB, Stock, util
from typing import Dict, List, Optional, Tuple

__all__ = ["fetch_all_data",
           "MAX_CONCURRENCY",
           "REQUEST_DELAY_SEC",
           "MAX_RETRIES",
           "BACKOFF_BASE"]

MAX_RETRIES: int = 2            
MAX_CONCURRENCY: int = 3
BACKOFF_BASE: float = 2.0         
REQUEST_DELAY_SEC: float = 0.6  

RETRYABLE_ERROR_CODES = {162, 321, 354, 420}

# region : Helper Functions

def _compute_end_datetime_str(screening_date: dt.date) -> str:

    """
    Reproduit la logique historique:
      - Fin de fenêtre = lendemain 11:00 US/Eastern (extended hours).
      - Format IB: 'YYYYMMDD HH:MM:SS' (UTC).
    """

    next_day_11 = dt.datetime.combine(screening_date + dt.timedelta(days=1), dt.time(11, 0))
    utc_end = EASTERN_TZ.localize(next_day_11).astimezone(pytz.utc)
    return utc_end.strftime("%Y%m%d %H:%M:%S")

def _needed_duration_days(screening_date: dt.date) -> int:

    """
    Fenêtre minimale utile pour tes conditions:
      - J + J-1 (et éventuellement J-2 pour retrouver l'open 16h la veille).
      - 3 jours suffisent largement, vs 7 D auparavant.
    """

    return 3

def _bars_to_df(bars) -> pd.DataFrame:

    """
    Convertit la réponse ib_insync en DataFrame homogène:
      - Index en US/Eastern
      - Colonnes: Open, High, Low, Close
    """

    if not bars:
        return pd.DataFrame()

    df = util.df(bars)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    idx = df.index

    if idx.tz is None:
        df.index = idx.tz_localize("UTC").tz_convert(EASTERN_TZ)

    else:
        df.index = idx.tz_convert(EASTERN_TZ)

    return df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"})

def _parse_ib_error_code(exc: Exception) -> Optional[int]:

    """
    Tente d'extraire un code erreur IB à partir du message d'exception.
    ib_insync n'expose pas toujours un objet structuré ; on scanne la chaîne.
    """

    msg = str(exc).lower()

    for code in RETRYABLE_ERROR_CODES:

        if f"error {code}" in msg or f"code {code}" in msg:
            return code
        
    return None

async def _qualify_like_before(ib: IB, ticker: str) -> Optional[Stock]:

    """
    Logique contrat strictement identique à l'ancien code:
      Stock(ticker, "SMART", "USD") + qualification.
    """

    try:
        contract = Stock(ticker, "SMART", "USD")
        await ib.qualifyContractsAsync(contract)
        return contract
    
    except Exception as e:
        logger.error(f"[QUALIFY] {ticker}: {e}")
        return None

async def _fetch_one(ib: IB, contract: Stock, end_time_str: str, *, screening_date: dt.date, delay_after: float = REQUEST_DELAY_SEC, max_retries: int = MAX_RETRIES) -> Tuple[str, pd.DataFrame]:

    """
    Récupère l'historique d'un contrat avec retries/backoff, en conservant:
      - barSizeSetting="1 hour"
      - whatToShow="TRADES"
      - useRTH=False
      - formatDate=1
    Retourne (symbol, DataFrame).
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

                backoff = BACKOFF_BASE ** (attempt - 1) 

                logger.warning(
                    f"[RETRY {attempt}/{max_retries}] {symbol} "
                    f"(code={err_code}): {e} — backoff {backoff:.2f}s")
                
                continue

            logger.error(f"[FETCH_FAIL] {symbol}: {e}")
            return symbol, pd.DataFrame()

# endregion

# region : API 

async def fetch_all_data(ib: IB, tickers: List[str], screening_date: dt.date) -> Dict[str, pd.DataFrame]:

    """
    - Construit/qualifie les contrats comme avant (SMART/USD).
    - EndDateTime = lendemain 11:00 ET.
    - Historique **3 D**, 1h, TRADES, useRTH=False, formatDate=1.
    - Qualification en parallèle (raisonnable).
    - Concurrence limitée + pacing + retries/backoff.
    Retourne: { "TICKER": DataFrame, ... }
    """

    if not tickers:
        logger.warning("[FETCH] Aucun ticker fourni.")
        return {}

    end_time_str = _compute_end_datetime_str(screening_date)
    logger.info(f"[FETCH] endDateTime={end_time_str} (US/Eastern +1j 11:00)")

    async def _qualify_one(t: str) -> tuple[str, Optional[Stock]]:
        c = await _qualify_like_before(ib, t)
        return t, c

    pairs = await asyncio.gather(*[_qualify_one(t) for t in tickers])
    qualified: Dict[str, Optional[Stock]] = {t: c for t, c in pairs}

    for t, c in pairs:
        if c is None:
            logger.warning(f"[SKIP] {t}: contrat non qualifié")

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results: Dict[str, pd.DataFrame] = {}

    async def _worker(ticker: str, contract: Optional[Stock]) -> None:

        if contract is None:
            results[ticker] = pd.DataFrame()
            return
        
        async with sem:
            sym, df = await _fetch_one(ib,
                                       contract,
                                       end_time_str,
                                       screening_date=screening_date)
       
            results[sym] = df

    tasks = [_worker(t, c) for t, c in qualified.items()]
    await asyncio.gather(*tasks, return_exceptions=False)

    ok = sum(1 for df in results.values() if not df.empty)
    ko = len(results) - ok

    logger.info(f"[FETCH_DONE] {ok} OK / {ko} KO sur {len(results)} tickers.")

    return results

# endregion
