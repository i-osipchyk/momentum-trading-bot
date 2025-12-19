import tkinter as tk
from pathlib import Path
from scanner.symbol_selector import SymbolSelector
from datetime import datetime, timezone

class App:
    def __init__(self, root):
        root.title("Momentum Scanner")

        base = Path(__file__).resolve().parent
        session_ts = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
        session = base / "data" / f"session_{session_ts}"
        session.mkdir(parents=True)

        self.selector = SymbolSelector(session, session_ts)
        self.selector.start()

root = tk.Tk()
App(root)
root.mainloop()
