# appp.py

from datetime import datetime
from tkinter import messagebox
from config import logger, MAX_TICKERS
from conditions import evaluate_conditions

from data_handler import (fetch_hourly_data,
                          get_data_for_date,
                          save_screener_results,
                          find_previous_day_data,
                          find_previous_16h_open)

def run_screener(app):

    app.tree.delete(*app.tree.get_children())
    app.results.clear()

    try:
        screening_date = app.date_entry.get()
        screening_date = datetime.strptime(screening_date, "%Y-%m-%d").date()

    except ValueError:
        messagebox.showerror("Error", "Invalid date format (YYYY-MM-DD)")
        return

    selected_tickers = [ticker for ticker, var in app.ticker_vars.items() if var.get()]

    if not selected_tickers:
        messagebox.showerror("Error", "No tickers selected.")
        return

    logger.info(f"Running screener for {screening_date} on {len(selected_tickers)} tickers.")

    for ticker in selected_tickers:

        logger.info(f"Processing ticker: {ticker}")
        df = fetch_hourly_data(ticker, screening_date)

        if df.empty:
            continue

        data_today = get_data_for_date(df, screening_date)
        data_yesterday = find_previous_day_data(df, screening_date)

        if data_today.empty or data_yesterday is None:
            logger.warning(f"Missing data for {ticker} on target or previous day.")
            continue

        open_16h = find_previous_16h_open(df, screening_date)

        if open_16h is None:
            logger.warning(f"No 16h open for {ticker} found.")
            continue

        passed = evaluate_conditions(app.conditions, data_today, open_16h, data_yesterday)

        if passed:

            serial = len(app.results) + 1
            ticker_no = app.tickers.index(ticker) + 1 if ticker in app.tickers else 0

            app.results.append((serial, ticker_no, ticker, open_16h))

    for serial, ticker_no, ticker, open_val in app.results:
        result_str = f"{serial}. TickerNo:{ticker_no} - {ticker} - Open16h: {open_val}"
        app.tree.insert("", "end", values=(result_str,))

    save_screener_results(app.results)

    messagebox.showinfo("Done", f"{len(app.results)} results found.\nSaved to file.")
    logger.info(f"Screener finished with {len(app.results)} matches.")