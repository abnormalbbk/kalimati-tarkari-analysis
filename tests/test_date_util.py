import pandas as pd
import pytest

from utils.date_util import parse_and_format_dates


def test_parse_and_format_dates():
    # Input with various formats and invalid date
    dates = pd.Series(['15/06/2023', '2023-07-01', '01-08-2023', 'invalid', None, '31/12/2020'])

    expected = pd.Series([
        '2023-06-15',
        '2023-07-01',
        '2023-01-08',
        pd.NaT,
        pd.NaT,
        '2020-12-31'
    ])

    result = parse_and_format_dates(dates)

    # Compare element-wise: NaT won't equal NaT, so check separately
    for r, e in zip(result, expected):
        if pd.isna(e):
            assert pd.isna(r)
        else:
            assert r == e


if __name__ == "__main__":
    pytest.main()
