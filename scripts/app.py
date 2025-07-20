import pandas as pd

from config import logger
from ib_insync import util
from datetime import datetime
from tkinter import messagebox
from fetch_data import fetch_all_data
from conditions import evaluate_conditions
from data_handler import (get_data_for_date,
                          save_screener_results,
                          find_previous_day_data,
                          find_previous_16h_open)

def run_screener(app):

    app.tree.delete(*app.tree.get_children())
    app.results.clear()

    try:
        screening_date = datetime.strptime(app.date_entry.get(), "%Y-%m-%d").date()

    except ValueError:
        messagebox.showerror("Error", "Invalid date format (YYYY-MM-DD)")
        return

    selected_tickers = [ticker for ticker, var in app.ticker_vars.items() if var.get()]

    if not selected_tickers:
        messagebox.showerror("Error", "No tickers selected.")
        return

    logger.info(f"[RUN] Fetching data for {len(selected_tickers)} tickers...")

    try:
        df_map = util.run(fetch_all_data(app.ib, selected_tickers, screening_date))

    except Exception as e:
        logger.error(f"[FETCH_ERROR] {e}")
        messagebox.showerror("Error", f"Data fetch failed: {e}")
        return

    results_append = app.results.append
    ticker_index = {ticker: idx + 1 for idx, ticker in enumerate(app.tickers)}

    for ticker in selected_tickers:

        df = df_map.get(ticker)

        if df is None or df.empty:
            logger.debug(f"[SKIP] No data for {ticker}")
            continue

        # Pre-fetch all required data
        data_today = get_data_for_date(df, screening_date)
        
        if data_today.empty:
            logger.debug(f"[SKIP] No data_today for {ticker}")
            continue

        data_yesterday = find_previous_day_data(df, screening_date)
        open_16h = find_previous_16h_open(df, screening_date)

        if data_yesterday is None:
            logger.debug(f"[SKIP] No previous day data for {ticker}")
            continue

        if open_16h is None:
            logger.debug(f"[SKIP] No 16h open found for {ticker}")
            continue

        if evaluate_conditions(app.conditions, data_today, open_16h, data_yesterday):
            results_append((len(app.results) + 1, ticker_index.get(ticker, 0), ticker, open_16h))

    for serial, ticker_no, ticker, open_val in app.results:
        app.tree.insert("", "end", values=(f"{serial}. TickerNo:{ticker_no} - {ticker} - Open16h: {open_val:.2f}",))

    save_screener_results(app.results)
    logger.info(f"[DONE] Screener finished with {len(app.results)} matches.")
    messagebox.showinfo("Done", f"{len(app.results)} results found.\nSaved to file.")
