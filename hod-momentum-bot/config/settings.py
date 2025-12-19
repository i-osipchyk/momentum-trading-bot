# ----- TRADE ENGINE -----
MODE = "exploratory"   # exploratory/real

ACCEPTANCE_BARS = 2
REJECTION_BARS = 2
MAX_TRADE_DURATION_SEC = 300

# exploratory TP / SL
EXP_TP_PCT = 0.015
EXP_SL_PCT = 0.005

# real trading hard SL
HARD_SL_PCT = 0.005

PRICE_LOOKBACK_SEC = 5
EXPLORATORY_TRACK_SEC = [5, 10, 20, 40, 60]


# ----- COLLECTORS -----
BINANCE_FUTURES_WS = "wss://fstream.binance.com/stream"
