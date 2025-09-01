from unittest.mock import patch

import pandas as pd

from models.etf_data import get_etf_data


def test_get_etf_data_fallback():
    sample = pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", periods=3),
            "Close": [1.0, 2.0, 3.0],
        }
    ).set_index("Date")

    with patch("models.etf_data.ak.fund_etf_hist_em", side_effect=Exception("403")):
        with patch("models.etf_data.yf.download", return_value=sample):
            df, symbol = get_etf_data("510300")
            assert symbol == "510300"
            assert list(df.columns) == ["close"]
            assert len(df) == 3

