import operator
import pandas as pd

from config import logger
from typing import List, Tuple, Optional

from utils import (get_range_stat,
                   get_first_n_bars,
                   safe_compare_bars,
                   safe_high_vs_open16,
                   safe_compare_to_range,
                   safe_range_high_vs_yclose,
                   safe_compare_day1_vs_today,
                   get_bar_at_hour as get_bar)

# region : Condition List

# Format: (id_condition, "text condition")
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

# endregion

# region : Conditions logic

CONDITION_FUNCTIONS = {}

# Conditions 1–2: Close >= Open on DAY-1 at 18h and 19h
for cid, h in zip(range(1, 3), [18, 19]):
    CONDITION_FUNCTIONS[cid] = lambda d, dy, *_ , h=h: ((b := get_bar(dy, h)) is not None and b["Close"] >= b["Open"],
                                                        (b := get_bar(dy, h)) is not None and b["Close"] < b["Open"])

# Conditions 3–18: Close >= Open from 4h to 19h
for cid, h in zip(range(3, 19), range(4, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: ((b := get_bar(d, h)) is not None and b["Close"] >= b["Open"],
                                                    (b := get_bar(d, h)) is not None and b["Close"] < b["Open"])

# Condition 19: Low 4h <= Low 19h DAY-1
CONDITION_FUNCTIONS[19] = lambda d, dy, *_: safe_compare_day1_vs_today(d, dy, 4, 19, "Low", operator.le, 19)

# Conditions 20–34: Low progression (Low h <= Low h-1)
for cid, (h1, h2) in zip(range(20, 35), zip(range(5, 20), range(4, 19))):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h1=h1, h2=h2, cid=cid: safe_compare_bars(d, h1, h2, "Low", operator.le, cid)

# Conditions 35–46: High h >= max High 4–15
for cid, h in zip(range(35, 47), range(4, 16)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: safe_compare_to_range(d, h, "High", range(4, 16), operator.ge)

# Conditions 47–50: High h >= max High 4–19
for cid, h in zip(range(47, 51), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: safe_compare_to_range(d, h, "High", range(4, 20), operator.ge)

# Condition 51: High 4h >= High 19h DAY-1
CONDITION_FUNCTIONS[51] = lambda d, dy, *_: safe_compare_day1_vs_today(d, dy, 4, 19, "High", operator.ge, 51)

# Conditions 52–66: High h >= High h-1
for cid, (h1, h2) in zip(range(52, 67), zip(range(5, 20), range(4, 19))):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h1=h1, h2=h2, cid=cid: safe_compare_bars(d, h1, h2, "High", operator.ge, cid)

# Condition 67: High 10h > max High 4–9
CONDITION_FUNCTIONS[67] = lambda d, *_: safe_compare_to_range(d, 10, "High", range(4, 10), operator.gt)

# Condition 68: Low 10h < min Low 4–9
CONDITION_FUNCTIONS[68] = lambda d, *_: safe_compare_to_range(d, 10, "Low", range(4, 10), operator.lt, agg_func="min")

# Conditions 69–76: Open/Close != High/Low for 4h and 5h
for cid, h in zip(range(69, 77), [4]*4 + [5]*4):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h, i=cid % 4: ((b := get_bar(d, h)) is not None and (b["Open"] != b["Low"] if i == 1 else
                                                                                                     b["Open"] != b["High"] if i == 2 else
                                                                                                     b["Close"] != b["Low"] if i == 3 else
                                                                                                     b["Close"] != b["High"]), False)

# Conditions 77–79: First 3 bars close >= open
for i, cid in enumerate(range(77, 80)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , i=i: ((bars := get_first_n_bars(d, 3)).shape[0] > i and bars.iloc[i]["Close"] >= bars.iloc[i]["Open"],
                                                                                                      bars.iloc[i]["Close"] < bars.iloc[i]["Open"] if bars.shape[0] > i else None)

# Conditions 80–81: Low bar comparisons
CONDITION_FUNCTIONS[80] = lambda d, dy, *_: safe_compare_day1_vs_today(d, dy, 4, 19, "Low", operator.le)
CONDITION_FUNCTIONS[81] = lambda d, *_: safe_compare_bars(d, 5, 4, "Low", operator.le)

# Conditions 82–83: High 4h ≥ High [5;8], High 8h ≥ High [4;7]
CONDITION_FUNCTIONS[82] = lambda d, *_: safe_compare_to_range(d, 4, "High", range(5, 9), operator.ge)
CONDITION_FUNCTIONS[83] = lambda d, *_: safe_compare_to_range(d, 8, "High", range(4, 8), operator.ge)

# Conditions 84–85: High ≠ Low for 18h and 19h DAY-1
for cid, h in zip([84, 85], [18, 19]):
    CONDITION_FUNCTIONS[cid] = lambda d, dy, *_ , h=h: ((b := get_bar(dy, h)) is not None and b["High"] != b["Low"],
                                                                                              b["High"] == b["Low"] if b else None)

# Conditions 86–101: High ≠ Low from 4h to 19h
for cid, h in zip(range(86, 102), range(4, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: ((b := get_bar(d, h)) is not None and b["High"] != b["Low"],
                                                                                         b["High"] == b["Low"] if b else None)

# Conditions 102–107: First bar == h
for cid, h in zip(range(102, 108), range(4, 10)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: ((b := get_first_n_bars(d, 1)).shape[0] > 0 and b.index[0].hour == h,
                                                                                                   b.index[0].hour != h if b.shape[0] > 0 else None)

# Conditions 108–111: Open h == Low h (16–19)
for cid, h in zip(range(108, 112), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: ((b := get_bar(d, h)) is not None and b["Open"] == b["Low"],
                                                                                         b["Open"] != b["Low"] if b else None)

# Conditions 112–115: Open h == High h (16–19)
for cid, h in zip(range(112, 116), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: ((b := get_bar(d, h)) is not None and b["Open"] == b["High"],
                                                                                         b["Open"] != b["High"] if b else None)

# Conditions 116–119: Close h == Low h (16–19)
for cid, h in zip(range(116, 120), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: ((b := get_bar(d, h)) is not None and b["Close"] == b["Low"],
                                                                                         b["Close"] != b["Low"] if b else None)

# Conditions 120–123: Close h == High h (16–19)
for cid, h in zip(range(120, 124), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: ((b := get_bar(d, h)) is not None and b["Close"] == b["High"],
                                                                                         b["Close"] != b["High"] if b else None)

# Conditions 124–125: High in range > factor * Open 16h DAY-1
for cid, factor in zip([124, 125], [1.5, 1.7]):
    CONDITION_FUNCTIONS[cid] = lambda d, dy, open16, *_ , f=factor: safe_high_vs_open16(d, dy, open16, f)

# Condition 126: High 4–19 > 2 * Close 19h DAY-1
CONDITION_FUNCTIONS[126] = lambda d, dy, *_: safe_range_high_vs_yclose(d, dy, 19, 2)

# Conditions 127–138: Low h ≤ min Low [4;15]
for cid, h in zip(range(127, 139), range(4, 16)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: safe_compare_to_range(d, h, "Low", range(4, 16), operator.le, agg_func="min")

# Conditions 139–142: Low h ≤ min Low [4;19]
for cid, h in zip(range(139, 143), range(16, 20)):
    CONDITION_FUNCTIONS[cid] = lambda d, *_ , h=h: safe_compare_to_range(d, h, "Low", range(4, 20), operator.le, agg_func="min")

# endregion

# === Main Function === #

def evaluate_conditions(conditions: dict, data: pd.DataFrame, open_16h_day_minus1: float, data_day_minus1: Optional[pd.DataFrame]) -> bool:

    results = {}

    if not any(v.get() for v in conditions.values()):
        logger.info("No conditions selected")
        return True

    selected_ids = {int(k) for k, v in conditions.items() if not k.startswith("inv_") and v.get()}
    inverse_ids = {int(k.split("_")[1]) for k, v in conditions.items() if k.startswith("inv_") and v.get()}

    all_requested = selected_ids.union(inverse_ids)

    # === Check() Helper function === #

    def check(cid: int, cond_primary: bool, cond_inverse: Optional[bool] = None):

        primary_key = str(cid)
        inverse_key = f"inv_{cid}"

        def to_bool(val):

            if isinstance(val, pd.Series):
                if val.size == 1:
                    return bool(val.item())
                
                logger.warning(f"Ambiguous Series in condition {cid}. Use .any()/.all() explicitly.")
                return False
            
            return bool(val) if val is not None else False

        try:
            if primary_key in conditions and conditions[primary_key].get():
                results[primary_key] = to_bool(cond_primary)

            if inverse_key in conditions and conditions[inverse_key].get():
                results[inverse_key] = to_bool(cond_inverse if cond_inverse is not None else not cond_primary)

        except Exception as e:
            logger.error(f"Error evaluating condition {cid}: {e}")
            results[primary_key] = False
            results[inverse_key] = False

    # =============================== #

    for cid in sorted(all_requested):

        func = CONDITION_FUNCTIONS.get(cid)

        if func is None:

            logger.warning(f"No evaluation function defined for condition {cid}.")
            results[cid] = False
            continue

        try:
            cond_primary, cond_inverse = func(data, data_day_minus1, open_16h_day_minus1)
            check(cid, cond_primary, cond_inverse)

        except Exception as e:
            logger.error(f"Exception while evaluating condition {cid}: {e}")
            results[cid] = False

    if not results:
        logger.warning("No conditions evaluated — exclusion.")
        return False
    return all(results.get(k, False) for k, v in conditions.items() if v.get())
