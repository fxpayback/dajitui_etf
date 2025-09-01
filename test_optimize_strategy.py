from unittest.mock import patch

from calculate_volatility import optimize_grid_strategy


def test_optimize_grid_strategy_selects_best():
    with patch('calculate_volatility.backtest_grid_strategy') as mock_backtest:
        mock_backtest.side_effect = [
            {'final_equity': 110, 'return_pct': 10, 'trades': []},
            {'final_equity': 105, 'return_pct': 5, 'trades': []},
            {'final_equity': 120, 'return_pct': 20, 'trades': []},
            {'final_equity': 115, 'return_pct': 15, 'trades': []},
        ]
        best = optimize_grid_strategy(['AAA', 'BBB'], '2024-01-01', '2024-12-31', [10, 20], initial_capital=100)
        assert best['symbol'] == 'BBB'
        assert best['grid_levels'] == 10
        assert best['final_equity'] == 120
