import pandas as pd


def parse_and_format_dates(series: pd.Series) -> pd.Series:
    """
    Parse a pandas Series of date strings into standardized date strings formatted as 'YYYY-MM-DD'.

    This function attempts to parse each date string in the Series using the default pandas parser first,
    which correctly handles ISO 8601 formats (e.g., '2023-07-01'). If parsing fails, it retries parsing
    with the `dayfirst=True` option to handle ambiguous day/month formats commonly used outside the US
    (e.g., '15/06/2023' meaning 15th June 2023).

    If both parsing attempts fail, the function returns `NaT` (Not a Time) for that entry.

    The output Series contains date strings in the ISO format 'YYYY-MM-DD' or `NaT` for invalid dates.

    Parameters:
    -----------
    series : pd.Series
        A pandas Series containing date strings to be parsed.

    Returns:
    --------
    pd.Series
        A pandas Series with dates formatted as strings 'YYYY-MM-DD' or `NaT` for unparsable entries.

    Example:
    --------
    >>> dates = pd.Series(['15/06/2023', '2023-07-01', '01-08-2023', 'invalid', None])
    >>> parse_and_format_dates(dates)
    0    2023-06-15
    1    2023-07-01
    2    2023-08-01
    3           NaT
    4           NaT
    dtype: object
    """

    def try_parse_format(date_str):
        if date_str is None or (isinstance(date_str, float) and pd.isna(date_str)):
            return pd.NaT

        try:
            # Try parsing with default settings (ISO formats)
            dt = pd.to_datetime(date_str, errors='raise')
        except Exception:
            try:
                # If it fails, try with dayfirst=True (common outside US)
                dt = pd.to_datetime(date_str, dayfirst=True, errors='raise')
            except Exception:
                # If both fail, return NaT
                return pd.NaT

        if pd.isna(dt):
            return pd.NaT

        return dt.strftime("%Y-%m-%d")

    return series.apply(try_parse_format)
