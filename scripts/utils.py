import operator
import pandas as pd

from typing import Optional

# === Evaluation function === #

def safe_high_vs_open16(d, dy, open16, factor):

    highs = [get_bar_at_hour(dy, h)["High"] for h in range(16, 20) if (bar := get_bar_at_hour(dy, h)) is not None]
    highs += [get_bar_at_hour(d, h)["High"] for h in range(4, 20) if (bar := get_bar_at_hour(d, h)) is not None]

    if highs:
        m = max(highs)
        return m > factor * open16, m <= factor * open16
    
    return False, False

def safe_bool_evaluation(value) -> Optional[bool]:

    """
    Tente de convertir proprement une valeur vers un booléen.
    Gère aussi les Series pandas d’un seul élément.
    """

    if value is None:
        return None

    try:
        if hasattr(value, "size") and value.size == 1:
            value = value.item()

        return bool(value)
    
    except Exception:
        return None

def safe_range_high_vs_yclose(d, dy, y_hour, mult):

    y = get_bar_at_hour(dy, y_hour)
    m = get_range_stat(d, range(4, 20), "High", "max")

    if y is not None and m is not None:
        return m > mult * y["Close"], m <= mult * y["Close"]
    
    return False, False

def safe_compare_bars(d, h1, h2, col, op, cid=None):

    b1, b2 = get_bar_at_hour(d, h1), get_bar_at_hour(d, h2)

    if b1 is not None and b2 is not None:

        primary = op(b1[col], b2[col])

        def inverse_condition_operator(op):

            symmetrical = {operator.le: operator.ge, operator.ge: operator.le}

            logical = {operator.le: operator.gt,
                       operator.lt: operator.ge,
                       operator.ge: operator.lt,
                       operator.gt: operator.le,
                       operator.eq: operator.ne,
                       operator.ne: operator.eq}

            if cid in list(range(19, 35)) + list(range(51, 67)):
                return symmetrical.get(op, op)
            
            return logical.get(op, lambda a, b: not op(a, b))

        inverse = inverse_condition_operator(op)(b1[col], b2[col])
        return primary, inverse

    return False, False

def safe_compare_to_range(d, h, col, hour_range, op, agg_func="max"):

    b = get_bar_at_hour(d, h)
    m = get_range_stat(d, hour_range, col, agg_func)

    if b is not None and m is not None:
        return op(b[col], m), not op(b[col], m)
    
    return False, False

def safe_compare_day1_vs_today(d, dy, h_today, h_yesterday, col, op, cid=None):

    b1 = get_bar_at_hour(d, h_today)
    b2 = get_bar_at_hour(dy, h_yesterday)

    if b1 is not None and b2 is not None:

        primary = op(b1[col], b2[col])

        def inverse_condition_operator(op):

            symmetrical = {operator.le: operator.ge, operator.ge: operator.le}

            logical = {operator.le: operator.gt,
                       operator.lt: operator.ge,
                       operator.ge: operator.lt,
                       operator.gt: operator.le,
                       operator.eq: operator.ne,
                       operator.ne: operator.eq,}

            if cid in [19, 51]:
                return symmetrical.get(op, op)
            
            return logical.get(op, lambda a, b: not op(a, b))

        inverse = inverse_condition_operator(op)(b1[col], b2[col])
        return primary, inverse
    return False, False

# === Text extraction functions === #

def extract_comparator(condition_text: str) -> str:

    """
    Extrait un opérateur de comparaison d'une chaîne.
    """

    for symbol in ["≥", "≤", "≠", "=", ">", "<"]:

        if symbol in condition_text:
            return symbol
        
    return "?"

def inverse_comparator(symbol: str) -> str:

    """
    Retourne l'opposé logique d'un comparateur.
    """

    inversions = {"≥": "<",
                  "≤": ">",
                  "=": "≠",
                  "≠": "=",
                  ">": "≤",
                  "<": "≥"}
    
    return symbol.translate(str.maketrans(inversions)) if any(k in symbol for k in inversions) else f"Inverse({symbol})"

# === Stock values analysis functions === #

def get_range_stat(df, hour_range: range, col: str, mode: str = "max"):

    """
    Renvoie max ou min d'une colonne (Open, High, Low, Close) sur une plage horaire donnée.
    - df : DataFrame avec index horaire
    - hour_range : range(4, 16), etc.
    - col : "High", "Low", ...
    - mode : "max" ou "min"
    """

    values = []

    for h in hour_range:

        bar = get_bar_at_hour(df, h)

        if bar is not None and col in bar:
            values.append(bar[col])

    if not values:
        return None
    
    return max(values) if mode == "max" else min(values)

def get_bar_at_hour(df, hour: int):

    """
    Returns the last hourly bar (as a Series) for a given hour.
    """

    try:
        bars = df.between_time(f"{hour:02d}:00", f"{hour:02d}:59")

        if not bars.empty:
            return bars.iloc[-1]  

    except Exception as e:
        print(f"[DEBUG] Error in get_bar_at_hour({hour}): {e}")

    return None

def get_first_n_bars(df, n: int):

    """
    Retourne les n premières lignes d’un DataFrame trié par date.
    """

    return df.sort_index().head(n)
