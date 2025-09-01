import operator
import pandas as pd

from config import logger, EASTERN_TZ
from typing import List, Tuple, Optional, Dict, Callable
from utils import (get_first_n_bars, safe_compare_bars, safe_high_vs_open16,
                                                        safe_compare_to_range,
                                                        safe_range_high_vs_yclose,
                                                        safe_compare_day1_vs_today,
                                                        get_bar_at_hour as get_bar)

CONDITION_DEFINITIONS: List[Tuple[int, str]] = [
    
    (1, "Close 18h DAY-1 ≥ Open 18h DAY-1"), (2, "Close 19h DAY-1 ≥ Open 19h DAY-1"), (3, "Close 4h ≥ Open 4h"), (4, "Close 5h ≥ Open 5h"), (5, "Close 6h ≥ Open 6h"),
    (6, "Close 7h ≥ Open 7h"), (7, "Close 8h ≥ Open 8h"), (8, "Close 9h ≥ Open 9h"), (9, "Close 10h ≥ Open 10h"), (10, "Close 11h ≥ Open 11h"),
    (11, "Close 12h ≥ Open 12h"), (12, "Close 13h ≥ Open 13h"), (13, "Close 14h ≥ Open 14h"), (14, "Close 15h ≥ Open 15h"), (15, "Close 16h ≥ Open 16h"), 
    (16, "Close 17h ≥ Open 17h"), (17, "Close 18h ≥ Open 18h"), (18, "Close 19h ≥ Open 19h"), (19, "Low 4h ≤ Low 19h DAY-1"), (20, "Low 5h ≤ Low 4h"), 
    (21, "Low 6h ≤ Low 5h"), (22, "Low 7h ≤ Low 6h"), (23, "Low 8h ≤ Low 7h"), (24, "Low 9h ≤ Low 8h"), (25, "Low 10h ≤ Low 9h"),
    (26, "Low 11h ≤ Low 10h"), (27, "Low 12h ≤ Low 11h"), (28, "Low 13h ≤ Low 12h"), (29, "Low 14h ≤ Low 13h"), (30, "Low 15h ≤ Low 14h"), 
    (31, "Low 16h ≤ Low 15h"), (32, "Low 17h ≤ Low 16h"), (33, "Low 18h ≤ Low 17h"), (34, "Low 19h ≤ Low 18h"), (35, "High 4h ≥ High [4;15]"), 
    (36, "High 5h ≥ High [4;15]"), (37, "High 6h ≥ High [4;15]"), (38, "High 7h ≥ High [4;15]"), (39, "High 8h ≥ High [4;15]"), (40, "High 9h ≥ High [4;15]"),
    (41, "High 10h ≥ High [4;15]"), (42, "High 11h ≥ High [4;15]"), (43, "High 12h ≥ High [4;15]"), (44, "High 13h ≥ High [4;15]"), (45, "High 14h ≥ High [4;15]"), 
    (46, "High 15h ≥ High [4;15]"), (47, "High 16h ≥ High [4;19]"), (48, "High 17h ≥ High [4;19]"), (49, "High 18h ≥ High [4;19]"), (50, "High 19h ≥ High [4;19]"),
    (51, "High 4h ≥ High 19h DAY-1"), (52, "High 5h ≥ High 4h"), (53, "High 6h ≥ High 5h"), (54, "High 7h ≥ High 6h"), (55, "High 8h ≥ High 7h"), 
    (56, "High 9h ≥ High 8h"), (57, "High 10h ≥ High 9h"), (58, "High 11h ≥ High 10h"), (59, "High 12h ≥ High 11h"), (60, "High 13h ≥ High 12h"),
    (61, "High 14h ≥ High 13h"), (62, "High 15h ≥ High 14h"), (63, "High 16h ≥ High 15h"), (64, "High 17h ≥ High 16h"), (65, "High 18h ≥ High 17h"), 
    (66, "High 19h ≥ High 18h"), (67, "High 10h > High [4;9]"), (68, "Low 10h < Low [4;9]"), (69, "Open 4h ≠ Low 4h"), (70, "Open 4h ≠ High 4h"), 
    (71, "Close 4h ≠ Low 4h"), (72, "Close 4h ≠ High 4h"), (73, "Open 5h ≠ Low 5h"), (74, "Open 5h ≠ High 5h"), (75, "Close 5h ≠ Low 5h"), 
    (76, "Close 5h ≠ High 5h"), (77, "First bar : Close ≥ Open"), (78, "Second bar : Close ≥ Open"), (79, "Third bar : Close ≥ Open"), (80, "Low First bar ≤ Low 19h DAY-1"), 
    (81, "Low Second bar ≤ Low First bar"), (82, "High 4h ≥ High [5;8]"), (83, "High 8h ≥ High [4;7]"), (84, "High 18h DAY-1 ≠ Low 18h DAY-1"), (85, "High 19h DAY-1 ≠ Low 19h DAY-1"),
    (86, "High 4h ≠ Low 4h"), (87, "High 5h ≠ Low 5h"), (88, "High 6h ≠ Low 6h"), (89, "High 7h ≠ Low 7h"), (90, "High 8h ≠ Low 8h"), 
    (91, "High 9h ≠ Low 9h"), (92, "High 10h ≠ Low 10h"), (93, "High 11h ≠ Low 11h"), (94, "High 12h ≠ Low 12h"), (95, "High 13h ≠ Low 13h"), 
    (96, "High 14h ≠ Low 14h"), (97, "High 15h ≠ Low 15h"), (98, "High 16h ≠ Low 16h"), (99, "High 17h ≠ Low 17h"), (100, "High 18h ≠ Low 18h"),
    (101, "High 19h ≠ Low 19h"), (102, "First bar = 4h"), (103, "First bar = 5h"), (104, "First bar = 6h"), (105, "First bar = 7h"), 
    (106, "First bar = 8h"), (107, "First bar = 9h"), (108, "Open 16h = Low 16h"), (109, "Open 17h = Low 17h"), (110, "Open 18h = Low 18h"), 
    (111, "Open 19h = Low 19h"), (112, "Open 16h = High 16h"), (113, "Open 17h = High 17h"), (114, "Open 18h = High 18h"), (115, "Open 19h = High 19h"),
    (116, "Close 16h = Low 16h"), (117, "Close 17h = Low 17h"), (118, "Close 18h = Low 18h"), (119, "Close 19h = Low 19h"), (120, "Close 16h = High 16h"), 
    (121, "Close 17h = High 17h"), (122, "Close 18h = High 18h"), (123, "Close 19h = High 19h"), (124, "High [16h DAY-1 ; 19h DAY] > 1.5 * Open 16h DAY-1"), (125, "High [16h DAY-1 ; 19h DAY] > 1.7 * Open 16h DAY-1"),
    (126, "High [4h DAY ; 19h DAY] > 2 * Close 19h DAY-1"), (127, "Low 4h ≤ Low [4;15]"), (128, "Low 5h ≤ Low [4;15]"), (129, "Low 6h ≤ Low [4;15]"), (130, "Low 7h ≤ Low [4;15]"), 
    (131, "Low 8h ≤ Low [4;15]"), (132, "Low 9h ≤ Low [4;15]"), (133, "Low 10h ≤ Low [4;15]"), (134, "Low 11h ≤ Low [4;15]"), (135, "Low 12h ≤ Low [4;15]"),
    (136, "Low 13h ≤ Low [4;15]"), (137, "Low 14h ≤ Low [4;15]"), (138, "Low 15h ≤ Low [4;15]"), (139, "Low 16h ≤ Low [4;19]"), (140, "Low 17h ≤ Low [4;19]"), 
    (141, "Low 18h ≤ Low [4;19]"), (142, "Low 19h ≤ Low [4;19]")]

# region : Time Functions

def _hour_bounds_from_df(df: pd.DataFrame, hour: int):

    last = df.index.max()

    if last is None:
        return None, None
    
    last_et = last.tz_convert(EASTERN_TZ) if last.tzinfo else EASTERN_TZ.localize(last)
    day_start = last_et.replace(hour=0, minute=0, second=0, microsecond=0)

    t0 = day_start + pd.Timedelta(hours=hour)
    t1 = t0 + pd.Timedelta(hours=1)

    return t0, t1

def _hour_window_today(df: pd.DataFrame, hour: int) -> pd.DataFrame:

    if df is None or df.empty:
        return pd.DataFrame()
    
    idx = df.index.tz_convert(EASTERN_TZ) if df.index.tz is not None else df.index.tz_localize(EASTERN_TZ)

    df = df.copy(); df.index = idx
    t0, t1 = _hour_bounds_from_df(df, hour)

    if t0 is None:
        return pd.DataFrame()
    
    return df.loc[(df.index >= t0) & (df.index < t1)]

def _make_hour_bar(win: pd.DataFrame) -> Optional[dict]:

    if win is None or win.empty:
        return None
    
    for c in ("Open", "High", "Low", "Close"):
        if c not in win:
            return None
        
    o = win["Open"].iloc[0]
    h = win["High"].max()
    l = win["Low"].min()
    c = win["Close"].iloc[-1]

    if pd.isna(o) or pd.isna(h) or pd.isna(l) or pd.isna(c):
        return None
    
    if "Volume" in win:

        vol = win["Volume"].sum()

        if pd.isna(vol) or vol <= 0:
            return None
        
    return {"Open": float(o), "High": float(h), "Low": float(l), "Close": float(c)}

def _safe_bar_at_hour_today(df: pd.DataFrame, h: int) -> Optional[dict]:

    if df is None or df.empty:
        return None
    
    try:
        b = get_bar(df, h)

        if b is not None and all(k in b for k in ("Open","High","Low","Close")) \
           and not any(pd.isna(b[k]) for k in ("Open","High","Low","Close")) \
           and (("Volume" not in b) or (not pd.isna(b["Volume"]) and b["Volume"] > 0)):
            return b
        
    except Exception:
        pass

    return _make_hour_bar(_hour_window_today(df, h))

# endregion

# region : Helper Functions

def _cond_bar_cmp_today(h: int, col_a: str, op: Callable, col_b: str):

    def _fn(d, *_):

        b = _safe_bar_at_hour_today(d, h)

        if b is None:
            return None, None
        
        cond = bool(op(b[col_a], b[col_b]))
        return cond, (not cond)
    
    return _fn

def _cond_bar_neq_today(h: int):
    return _cond_bar_cmp_today(h, "High", operator.ne, "Low")

def _cond_day1_neq(h: int):

    def _fn(d, dy, *_):
       
        b = _safe_bar_at_hour_today(dy, h)

        if b is None:
            return None, None
        
        cond = bool(b["High"] != b["Low"])
        return cond, (not cond)
    
    return _fn

def _cond_open_close_rel_today(h: int, which: str, target: str, op: Callable):
    col = "Open" if which == "open" else "Close"
    return _cond_bar_cmp_today(h, col, op, target)

def _cond_first_bar_is(h: int):

    def _fn(d, *_):

        bars = get_first_n_bars(d, 1)

        if bars.shape[0] == 0:
            return None, None
        
        cond = (bars.index[0].tz_convert(EASTERN_TZ).hour == h) if bars.index[0].tzinfo else (bars.index[0].hour == h)
        return cond, (not cond)
    
    return _fn

# endregion

# region : Conditions evaluation

CONDITION_FUNCTIONS: Dict[int, Callable] = {}

# 1–2 (DAY-1 Close >= Open)
for cid, h in zip(range(1, 3), [18, 19]):
    CONDITION_FUNCTIONS[cid] = lambda d, dy, *_ , h=h: (
        (b := _safe_bar_at_hour_today(dy, h)) is not None and b["Close"] >= b["Open"],
        (b := _safe_bar_at_hour_today(dy, h)) is not None and b["Close"] <  b["Open"],
)

# 3–18 (J Close >= Open)
for cid, h in zip(range(3, 19), range(4, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: (
        (b := _safe_bar_at_hour_today(d, h)) is not None and b["Close"] >= b["Open"],
        (b := _safe_bar_at_hour_today(d, h)) is not None and b["Close"] <  b["Open"],
    )

# 19
CONDITION_FUNCTIONS[19] = lambda d, dy, *_: safe_compare_day1_vs_today(d, dy, 4, 19, "Low", operator.le, 19)

# 20–34 (Low progression)
for cid, (h1, h2) in zip(range(20, 35), zip(range(5, 20), range(4, 19))):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h1=h1, h2=h2, cid=cid: safe_compare_bars(d, h1, h2, "Low", operator.le, cid)

# 35–46 / 47–50
for cid, h in zip(range(35, 47), range(4, 16)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: safe_compare_to_range(d, h, "High", range(4, 16), operator.ge)
for cid, h in zip(range(47, 51), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: safe_compare_to_range(d, h, "High", range(4, 20), operator.ge)

# 51
CONDITION_FUNCTIONS[51] = lambda d, dy, *_: safe_compare_day1_vs_today(d, dy, 4, 19, "High", operator.ge, 51)

# 52–66 (High progression)
for cid, (h1, h2) in zip(range(52, 67), zip(range(5, 20), range(4, 19))):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h1=h1, h2=h2, cid=cid: safe_compare_bars(d, h1, h2, "High", operator.ge, cid)

# 67–68
CONDITION_FUNCTIONS[67] = lambda d, *_: safe_compare_to_range(d, 10, "High", range(4, 10), operator.gt)
CONDITION_FUNCTIONS[68] = lambda d, *_: safe_compare_to_range(d, 10, "Low",  range(4, 10), operator.lt, agg_func="min")

# 69–76 (Open/Close != High/Low) 4h/5h
def _make_cond_neq(h: int, idx_mod4: int):
    def _fn(d, *_):
        b = _safe_bar_at_hour_today(d, h)
        if b is None:
            return False, False
        if idx_mod4 == 1:
            cond = b["Open"] != b["Low"]
        elif idx_mod4 == 2:
            cond = b["Open"] != b["High"]
        elif idx_mod4 == 3:
            cond = b["Close"] != b["Low"]
        else:
            cond = b["Close"] != b["High"]
        return cond, (not cond)
    return _fn
for cid, h in zip(range(69, 77), [4]*4 + [5]*4):
    CONDITION_FUNCTIONS[cid] = _make_cond_neq(h, cid % 4)

# 77–79
for i, cid in enumerate(range(77, 80)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , i=i: (
        (bars := get_first_n_bars(d, 3)).shape[0] > i and bars.iloc[i]["Close"] >= bars.iloc[i]["Open"],
        (bars.iloc[i]["Close"] < bars.iloc[i]["Open"]) if bars.shape[0] > i else None,
    )

# 80–81
CONDITION_FUNCTIONS[80] = lambda d, dy, *_: safe_compare_day1_vs_today(d, dy, 4, 19, "Low", operator.le, cid=80)
CONDITION_FUNCTIONS[81] = lambda d, *_: safe_compare_bars(d, 5, 4, "Low", operator.le, cid=81)

# 82–83
CONDITION_FUNCTIONS[82] = lambda d, *_: safe_compare_to_range(d, 4, "High", range(5, 9), operator.ge)
CONDITION_FUNCTIONS[83] = lambda d, *_: safe_compare_to_range(d, 8, "High", range(4, 8), operator.ge)

# 84–85 (DAY-1 High != Low)
for cid, h in zip([84, 85], [18, 19]):
    CONDITION_FUNCTIONS[cid] = _cond_day1_neq(h)

# 86–101 (DAY High != Low)
for cid, h in zip(range(86, 102), range(4, 20)):
    CONDITION_FUNCTIONS[cid] = _cond_bar_neq_today(h)

# 102–107 (first bar == h)
for cid, h in zip(range(102, 108), range(4, 10)):
    CONDITION_FUNCTIONS[cid] = _cond_first_bar_is(h)

# 108–123 (Open/Close vs High/Low, 16..19)
for cid, h in zip(range(108, 112), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = _cond_open_close_rel_today(h, "open",  "Low",  operator.eq)
for cid, h in zip(range(112, 116), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = _cond_open_close_rel_today(h, "open",  "High", operator.eq)
for cid, h in zip(range(116, 120), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = _cond_open_close_rel_today(h, "close", "Low",  operator.eq)
for cid, h in zip(range(120, 124), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = _cond_open_close_rel_today(h, "close", "High", operator.eq)

# 124–126 / 127–142
for cid, factor in zip([124, 125], [1.5, 1.7]):
    CONDITION_FUNCTIONS[cid] = lambda d, dy, open16, *_ , f=factor: safe_high_vs_open16(d, dy, open16, f)
CONDITION_FUNCTIONS[126] = lambda d, dy, *_: safe_range_high_vs_yclose(d, dy, 19, 2)
for cid, h in zip(range(127, 139), range(4, 16)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: safe_compare_to_range(d, h, "Low", range(4, 16), operator.le, agg_func="min")
for cid, h in zip(range(139, 143), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: safe_compare_to_range(d, h, "Low", range(4, 20), operator.le, agg_func="min")

# endregion

# ---------------------------
# Pre-check missing hours
# ---------------------------

def _preflight_missing_hours(conditions: dict, df: pd.DataFrame):

    wanted = set()

    for k, v in conditions.items():

        if not v.get():
            continue

        if k.startswith("inv_"):
            k = k.split("_")[1]

        cid = int(k)

        if 3 <= cid <= 18:    wanted.add(cid - (3 - 4))       
        if 86 <= cid <= 101:  wanted.add(cid - (86 - 4))      
        if 120 <= cid <= 123: wanted.add(cid - (120 - 16))   
        if 102 <= cid <= 107: wanted.add(cid - 98)            

    present = set()

    if df is not None and not df.empty:

        for h in range(0, 24):
            if _safe_bar_at_hour_today(df, h) is not None:
                present.add(h)

    missing = sorted(wanted - present)

    if missing:
        logger.warning(f"[PRECHECK] Missing hours on 'data' for today: {missing}")

# ---------------------------
# Evaluation function
# ---------------------------

def evaluate_conditions(conditions: dict,
                        data: pd.DataFrame,
                        open_16h_day_minus1: float,
                        data_day_minus1: Optional[pd.DataFrame]) -> bool:
    """
    Retourne True si toutes les conditions cochées ET évaluées sont vraies.
    Ignore les conditions N/A (barres absentes). Ignore les paires contradictoires.
    """

    if not any(v.get() for v in conditions.values()):
        logger.info("No conditions selected")
        return True

    _preflight_missing_hours(conditions, data)

    selected_ids = {int(k) for k, v in conditions.items() if not k.startswith("inv_") and v.get()}
    inverse_ids  = {int(k.split("_")[1]) for k, v in conditions.items() if k.startswith("inv_") and v.get()}

    both = selected_ids & inverse_ids

    if both:
        logger.warning(f"Ignoring contradictory pairs for ids: {sorted(both)}")

        for cid in list(both):
            selected_ids.discard(cid)
            inverse_ids.discard(cid)

    requested = sorted(selected_ids | inverse_ids)
    results: Dict[str, bool] = {}

    def record(key: str, value):

        if value is None:
            return
        
        results[key] = bool(value)

    for cid in requested:

        func = CONDITION_FUNCTIONS.get(cid)

        if func is None:
            logger.warning(f"No evaluation function defined for condition {cid}.")
            continue

        try:
            primary, inverse = func(data, data_day_minus1, open_16h_day_minus1)

        except Exception as e:
            logger.error(f"Exception while evaluating condition {cid}: {e}")
            continue

        pk, ik = str(cid), f"inv_{cid}"

        if pk in conditions and conditions[pk].get():
            record(pk, primary)

        if ik in conditions and conditions[ik].get():
            inv_val = inverse if inverse is not None else (None if primary is None else (not primary))
            record(ik, inv_val)

    picked_keys = [k for k, v in conditions.items() if v.get()]
    scored = [results[k] for k in picked_keys if k in results]

    if not scored:
        logger.warning("No conditions evaluated (N/A) — exclusion.")
        return False

    return all(scored)
