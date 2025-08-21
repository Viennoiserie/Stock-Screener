import tkinter as tk

from ib_connect import get_ib   
from gui_handler import StockScreenerApp
from config import APP_TITLE, ICON_PATH, DEFAULT_FULLSCREEN, DEFAULT_ZOOMED

def launch_app():

    root = tk.Tk()
    root.title(APP_TITLE)

    if DEFAULT_ZOOMED:
        root.state("zoomed")

    if DEFAULT_FULLSCREEN:
        root.attributes("-fullscreen", True)

    try:
        root.iconbitmap(ICON_PATH)

    except Exception as e:
        print(f"Could not load icon: {e}")

    ib = get_ib()

    if ib is None:
        print("Could not connect to IB Gateway, exiting.")
        return

    app = StockScreenerApp(root, ib) 
    root.mainloop()
    
if __name__ == "__main__":
    launch_app()
