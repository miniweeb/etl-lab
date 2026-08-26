# work/A03_transformation/a03_cleasers.py
"""Cleaning rules for the shopcore feed, one per known pathology (SPEC 3.5).

Job: return canonical SQL expressions (as strings) that later code embeds in
its queries with f-strings. This module is the single home of these rules --
A05's checks and A07's pipeline import it; nobody re-pastes the SQL.

Assumptions:
  - dirty columns arrive as VARCHAR (staging reads dirt as text so it can count it)
  - session TimeZone is 'UTC' (clean_signup_ts decodes epoch seconds)
  - ids fit in a DOUBLE without precision loss (ours < 1.2M; limit ~2^53)
"""

TS_FORMATS = ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S', '%Y-%m-%dT%H:%M:%S']

STATUS_VALUES = ('created', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded')
PAYMENT_VALUES = ('card', 'wallet', 'bank_transfer', 'cod', 'paypal')


def _sql_list(values) -> str:
    """('a','b') -> "'a', 'b'" -- values must not contain single quotes."""
    return ", ".join(f"'{v}'" for v in values)


def clean_order_ts(col: str = "order_ts") -> str:
    """Three known formats -> TIMESTAMP; anything else (incl. impossible dates) -> NULL."""
    return f"try_strptime({col}, [{_sql_list(TS_FORMATS)}])"


def clean_money(col: str = "order_total") -> str:
    """'93,32'/'1.234,56' (euro), '$79.98', 'N/A'/empty -> DECIMAL(14,2) or NULL.

    Negatives survive on purpose: sign is a validity problem (A05), not a format one.
    """
    return (f"TRY_CAST(NULLIF("
            f"CASE WHEN {col} LIKE '%,%' "
            f"THEN replace(replace({col}, '.', ''), ',', '.') "   # thousands-dots first
            f"ELSE replace(TRIM({col}), '$', '') END, "
            f"'N/A') AS DECIMAL(14,2))")


def clean_id(col: str = "customer_id") -> str:
    """Excel-float '123456.0' -> BIGINT; empty/garbage -> NULL. DOUBLE detour: portable."""
    return f"CAST(TRY_CAST({col} AS DOUBLE) AS BIGINT)"


def clean_enum(col: str, allowed: tuple) -> str:
    """TRIM + lower, then whitelist; junk -> NULL. A cleaner never guesses a category."""
    return (f"CASE WHEN lower(TRIM({col})) IN ({_sql_list(allowed)}) "
            f"THEN lower(TRIM({col})) END")


def clean_signup_ts(col: str = "signup_ts") -> str:
    """ISO / DD-MM-YYYY / epoch-seconds strings -> TIMESTAMP. Digits-only = epoch."""
    return (f"CASE WHEN {col} IS NULL THEN NULL "
            f"WHEN regexp_full_match({col}, '[0-9]+') "
            f"THEN CAST(to_timestamp(CAST({col} AS BIGINT)) AS TIMESTAMP) "
            f"ELSE try_strptime({col}, ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y']) END")


def clean_bool(col: str = "is_active") -> str:
    """Mixed booleans -> BOOLEAN; unknown -> NULL. Explicit whitelists on both sides."""
    return (f"CASE WHEN lower(TRIM({col})) IN ('true','1','yes','y','t') THEN true "
            f"WHEN lower(TRIM({col})) IN ('false','0','no','n','f') THEN false END")