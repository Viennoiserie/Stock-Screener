import tkinter as tk

from config import logger
from app import run_screener
from tkinter import ttk, filedialog, messagebox
from data_handler import get_screening_date_now
from utils import extract_comparator, inverse_comparator
from conditions import CONDITION_DEFINITIONS as cond_defs

class StockScreenerApp:

    def __init__(self, root, ib):

        self.ib = ib
        self.root = root
        self.root.title("Nasdaq Stock Screener")

        self.tickers = []
        self.results = []

        self.conditions = {}
        self.ticker_vars = {}

        self.create_widgets()
        self.setup_conditions()

    def create_widgets(self):

        self.main_frame = ttk.Frame(self.root, padding=5)
        self.main_frame.grid(row=0, column=0, sticky=tk.NSEW)

        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        self.main_frame.columnconfigure(0, weight=0)  
        self.main_frame.columnconfigure(1, weight=3)  
        self.main_frame.columnconfigure(2, weight=1)  
        self.main_frame.rowconfigure(0, weight=0)
        self.main_frame.rowconfigure(1, weight=1)

        # region : Tickers

        self.ticker_frame = ttk.LabelFrame(self.main_frame, text="Ticker Selection", padding=5, width=260)
        self.ticker_frame.grid(row=0, column=2, rowspan=2, sticky=tk.NS, padx=5, pady=5)
        self.ticker_frame.grid_propagate(False)

        self.ticker_frame.rowconfigure(1, weight=1)
        self.ticker_frame.columnconfigure(0, weight=1)

        ticker_btn_frame = ttk.Frame(self.ticker_frame)
        ticker_btn_frame.grid(row=0, column=0, sticky=tk.EW)
        ttk.Button(ticker_btn_frame, text="Select All", command=self.select_all_tickers).pack(side=tk.LEFT, padx=2)
        ttk.Button(ticker_btn_frame, text="Unselect All", command=self.unselect_all_tickers).pack(side=tk.LEFT, padx=2)

        canvas_frame = ttk.Frame(self.ticker_frame)
        canvas_frame.grid(row=1, column=0, sticky=tk.NSEW)
        canvas_frame.rowconfigure(0, weight=1)
        canvas_frame.columnconfigure(0, weight=1)

        ticker_scroll_canvas = tk.Canvas(canvas_frame)
        ticker_scroll_canvas.grid(row=0, column=0, sticky=tk.NSEW)

        ticker_scrollbar = ttk.Scrollbar(canvas_frame, orient="vertical", command=ticker_scroll_canvas.yview)
        ticker_scrollbar.grid(row=0, column=1, sticky=tk.NS)

        ticker_scroll_canvas.configure(yscrollcommand=ticker_scrollbar.set)

        self.ticker_inner_frame = ttk.Frame(ticker_scroll_canvas)
        self.ticker_inner_frame.bind("<Configure>", lambda e: ticker_scroll_canvas.configure(scrollregion=ticker_scroll_canvas.bbox("all")))

        ticker_scroll_canvas.create_window((0, 0), window=self.ticker_inner_frame, anchor="nw")

        def _on_mousewheel(event):
            ticker_scroll_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        ticker_scroll_canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # endregion

        # region : Results

        results_frame = ttk.LabelFrame(self.main_frame, text="Results", padding=5)
        results_frame.grid(row=1, column=0, sticky=tk.NSEW, padx=5, pady=5)

        self.tree = ttk.Treeview(results_frame, columns=("result",), show="headings")
        self.tree.heading("result", text="Results")
        self.tree.pack(fill=tk.BOTH, expand=True)

        # endregion

        # region : Controls

        control_frame = ttk.LabelFrame(self.main_frame, text="Controls", padding=5)
        control_frame.grid(row=0, column=0, sticky=tk.NW, padx=5, pady=5)

        ttk.Label(control_frame, text="Screening Date (YYYY-MM-DD):").grid(row=0, column=0, sticky=tk.W)

        self.date_entry = ttk.Entry(control_frame, width=12)
        self.date_entry.grid(row=0, column=1, sticky=tk.W, padx=5)
        self.date_entry.insert(0, get_screening_date_now().strftime("%Y-%m-%d"))

        ttk.Button(control_frame, text="Upload Ticker List", command=self.upload_file).grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=(5, 0))

        btn_frame = ttk.Frame(control_frame)
        btn_frame.grid(row=3, column=0, columnspan=2, pady=10)

        ttk.Button(btn_frame, text="Run Screener", command=lambda: run_screener(self)).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Reset", command=self.reset).pack(side=tk.LEFT, padx=5)

        # endregion

        # region : Indicators

        indicators_frame = ttk.LabelFrame(self.main_frame, text="Indicators", padding=5)
        indicators_frame.grid(row=0, column=1, rowspan=2, sticky=tk.NSEW, padx=5, pady=5)

        self.cond_scrollable = ttk.Frame(indicators_frame)
        self.cond_scrollable.pack(fill=tk.BOTH, expand=True)

        ttk.Button(self.cond_scrollable, text="Deselect All Indicators", command=self.deselect_all_conditions).pack(pady=0)

        # endregion

    # region : Helper Functions

    def reset(self):

        self.date_entry.delete(0, tk.END)
        self.date_entry.insert(0, get_screening_date_now().strftime("%Y-%m-%d"))
        self.tree.delete(*self.tree.get_children())

        for widget in self.ticker_inner_frame.winfo_children():
            widget.destroy()

        for var in self.conditions.values():
            var.set(False)

        for var in self.ticker_vars.values():
            var.set(False)

        self.tickers = []
        self.results = []

        logger.info("App reset.")

    def upload_file(self):

        path = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])

        if path:

            try:
                with open(path, "r") as f:
                    tickers = [x.strip().split(":")[-1] for x in f.read().strip().split(",") if x.strip()]

                self.tickers = tickers
                logger.info(f"Loaded tickers: {tickers}")

                messagebox.showinfo("Success", f"{len(tickers)} tickers loaded.")
                self.populate_ticker_selection()

            except Exception as e:
                logger.error(f"Failed to load tickers: {e}")
                messagebox.showerror("Error", str(e))

    def setup_conditions(self):

        notebook = ttk.Notebook(self.cond_scrollable)
        notebook.pack(fill=tk.BOTH, expand=True)

        tab1 = ttk.Frame(notebook)
        tab2 = ttk.Frame(notebook)

        notebook.add(tab1, text="Conditions 1 –> 101")
        notebook.add(tab2, text="Conditions 102 –> 142")

        def create_grid_conditions(tab, start_idx, end_idx):

            chunk_size = 34
            large_font = ("Arial", 9)

            style = ttk.Style()
            style.configure("Large.TLabel", font=large_font)
            style.configure("Large.TCheckbutton", font=large_font)

            for block_start in range(start_idx, end_idx, chunk_size):

                block_end = min(block_start + chunk_size, end_idx)
                block_idx = (block_start - start_idx) // chunk_size

                block_frame = ttk.LabelFrame(tab, text=f"Conditions {cond_defs[block_start][0]} – {cond_defs[block_end - 1][0]}")
                block_frame.grid(row=0, column=start_idx // 25 + block_idx, padx=10, pady=10, sticky="ns")

                current_row = 0

                for i in range(block_start, block_end):

                    idx, label = cond_defs[i]
                    comparator = extract_comparator(label)

                    if 19 <= idx <= 34 or 51 <= idx <= 66 or idx in (80, 81):
                        opposite_comparator = ("≤" if comparator == "≥" else "≥" if comparator == "≤" else comparator)
                        
                    else:
                        opposite_comparator = inverse_comparator(comparator)

                    ttk.Label(block_frame, text=f"{idx}. {label}", anchor="w", style="Large.TLabel").grid(row=current_row, column=0, sticky="w")
                    ttk.Label(block_frame, text=f"{comparator}").grid(row=current_row, column=1, sticky="e", padx=(5, 2))

                    var = tk.BooleanVar()
                    ttk.Checkbutton(block_frame, variable=var, style="Large.TCheckbutton").grid(row=current_row, column=2, sticky="w")

                    ttk.Label(block_frame, text=f"{opposite_comparator}").grid(row=current_row, column=3, sticky="e", padx=(10, 2))

                    inv_var = tk.BooleanVar()
                    ttk.Checkbutton(block_frame, variable=inv_var, style="Large.TCheckbutton").grid(row=current_row, column=4, sticky="w")

                    self.conditions[str(idx)] = var
                    self.conditions[f"inv_{idx}"] = inv_var

                    def make_callback(main=var, inverse=inv_var):

                        def callback(*args):

                            if main.get():
                                inverse.set(False)

                            elif inverse.get():
                                main.set(False)

                        return callback

                    current_row += 1

                    if idx in [18, 50, 66, 72, 76, 79, 81, 83, 107, 111, 115, 119, 123]:
                        ttk.Separator(block_frame, orient="horizontal").grid(row=current_row, column=0, columnspan=5, sticky="ew", pady=0)
                        current_row += 1

        create_grid_conditions(tab1, 0, 101)
        create_grid_conditions(tab2, 101, 126)
        create_grid_conditions(tab2, 126, len(cond_defs))

    def select_all_tickers(self):

        for var in self.ticker_vars.values():
            var.set(True)

    def unselect_all_tickers(self):

        for var in self.ticker_vars.values():
            var.set(False)

    def deselect_all_conditions(self):

        for key, var in self.conditions.items():
            var.set(False)

    def populate_ticker_selection(self):

        for widget in self.ticker_inner_frame.winfo_children():
            widget.destroy()

        self.ticker_vars = {}

        for i, ticker in enumerate(self.tickers, start=1):

            var = tk.BooleanVar(value=True)
            
            ttk.Checkbutton(self.ticker_inner_frame,
                            text=f"{i}. {ticker}",
                            variable=var).grid(row=i - 1, column=0, sticky=tk.W, pady=1)
            
            self.ticker_vars[ticker] = var

# endregion
