"""Portfolio optimization and backtesting using evolutionary algorithm.

This script evaluates multiple ETF portfolios by optimizing asset weights on
rolling training windows and comparing them via cross-validated Sharpe ratios.
The final portfolio is chosen based on validation performance and then
evaluated on a hold-out test segment to guard against overfitting.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence

import numpy as np
import pandas as pd

from models.etf_data import get_etf_data


@dataclass
class BacktestResult:
    """Container for backtest results."""

    symbols: Sequence[str]
    weights: np.ndarray
    train_sharpe: float
    test_sharpe: float
    validation_sharpe: float


def fetch_prices(symbols: Iterable[str]) -> pd.DataFrame:
    """Fetch and align closing prices for a list of ETF symbols."""
    frames = []
    for symbol in symbols:
        df, _ = get_etf_data(symbol)
        frames.append(df["close"].rename(symbol))
    prices = pd.concat(frames, axis=1).dropna()
    return prices


def sharpe_ratio(weights: np.ndarray, returns: pd.DataFrame) -> float:
    """Compute annualised Sharpe ratio for a set of weights."""
    portfolio_returns = returns.dot(weights)
    mean = portfolio_returns.mean()
    std = portfolio_returns.std()
    if std == 0:
        return 0.0
    return float(mean / std * np.sqrt(252))


def evolutionary_optimize(
    returns: pd.DataFrame,
    population_size: int = 40,
    generations: int = 60,
    mutation_rate: float = 0.1,
    rng: np.random.Generator | None = None,
) -> np.ndarray:
    """Optimise asset weights using a simple evolutionary algorithm.

    Parameters
    ----------
    returns:
        Historical return series used to evaluate fitness.
    population_size, generations, mutation_rate:
        Standard evolutionary algorithm hyper-parameters.
    rng:
        Optional random number generator for reproducible results.
    """
    rng = rng or np.random.default_rng()
    n_assets = returns.shape[1]

    def random_weights() -> np.ndarray:
        w = rng.random(n_assets)
        return w / w.sum()

    population = np.array([random_weights() for _ in range(population_size)])

    for _ in range(generations):
        fitness = np.array([sharpe_ratio(ind, returns) for ind in population])
        # Select top half of population
        selected_idx = np.argsort(fitness)[-population_size // 2 :]
        parents = population[selected_idx]

        # Create offspring via crossover and mutation
        children = []
        while len(children) < population_size - len(parents):
            p1, p2 = parents[rng.integers(len(parents), size=2)]
            mask = rng.random(n_assets) < 0.5
            child = np.where(mask, p1, p2)
            if rng.random() < mutation_rate:
                mutate_idx = rng.integers(n_assets)
                child[mutate_idx] = rng.random()
            child = child / child.sum()
            children.append(child)
        population = np.vstack((parents, children))

    # Return the individual with highest fitness
    fitness = np.array([sharpe_ratio(ind, returns) for ind in population])
    best_idx = fitness.argmax()
    return population[best_idx]


def backtest_portfolio(
    symbols: Sequence[str],
    n_splits: int = 3,
    seed: int | None = 42,
) -> BacktestResult:
    """Backtest a portfolio using walk-forward cross-validation.

    Parameters
    ----------
    symbols:
        ETF symbols to include in the portfolio.
    n_splits:
        Number of cross-validation folds. The data is divided into
        ``n_splits + 1`` chronological segments; the last segment is kept as
        an untouched test set.
    """

    prices = fetch_prices(symbols)
    returns = prices.pct_change().dropna()
    rng = np.random.default_rng(seed)

    # Determine fold size for walk-forward validation
    fold_size = len(returns) // (n_splits + 1)
    val_scores: List[float] = []

    # Perform walk-forward cross-validation
    for i in range(n_splits):
        train = returns.iloc[: fold_size * (i + 1)]
        val = returns.iloc[fold_size * (i + 1) : fold_size * (i + 2)]
        weights = evolutionary_optimize(train, rng=rng)
        val_scores.append(sharpe_ratio(weights, val))

    # Train on all data except the final segment and evaluate on the hold-out
    train = returns.iloc[: fold_size * n_splits]
    test = returns.iloc[fold_size * n_splits :]
    weights = evolutionary_optimize(train, rng=rng)
    train_score = sharpe_ratio(weights, train)
    test_score = sharpe_ratio(weights, test)

    return BacktestResult(
        symbols,
        weights,
        train_score,
        test_score,
        float(np.mean(val_scores)) if val_scores else 0.0,
    )


def optimize_portfolios(
    portfolios: Iterable[Sequence[str]],
    n_splits: int = 3,
    seed: int | None = 42,
) -> BacktestResult:
    """Evaluate multiple portfolios and return the best one.

    Portfolios are compared using the cross-validated Sharpe ratio to reduce
    the risk of overfitting to any particular sample.
    """
    results = [backtest_portfolio(p, n_splits=n_splits, seed=seed) for p in portfolios]
    return max(results, key=lambda r: r.validation_sharpe)


if __name__ == "__main__":
    # Example usage with two candidate portfolios
    candidate_portfolios: List[List[str]] = [
        ["510300", "510500"],
        ["159915", "159949", "159922"],
    ]
    best = optimize_portfolios(candidate_portfolios)
    print("Best portfolio based on cross-validated Sharpe ratio:")
    print(f"Symbols: {best.symbols}")
    print(f"Weights: {np.round(best.weights, 3)}")
    print(f"Train Sharpe: {best.train_sharpe:.3f}")
    print(f"Validation Sharpe: {best.validation_sharpe:.3f}")
    print(f"Test Sharpe: {best.test_sharpe:.3f}")
