"""Portfolio optimization and backtesting using evolutionary algorithm.

This script evaluates multiple ETF portfolios by optimizing asset weights on a
training set and selecting the portfolio that performs best on a test set.
It uses an evolutionary algorithm for fast convergence and includes a
train/test split to reduce the risk of overfitting.
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
) -> np.ndarray:
    """Optimise asset weights using a simple evolutionary algorithm."""
    n_assets = returns.shape[1]

    def random_weights() -> np.ndarray:
        w = np.random.rand(n_assets)
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
            p1, p2 = parents[np.random.randint(len(parents), size=2)]
            mask = np.random.rand(n_assets) < 0.5
            child = np.where(mask, p1, p2)
            if np.random.rand() < mutation_rate:
                mutate_idx = np.random.randint(n_assets)
                child[mutate_idx] = np.random.rand()
            child = child / child.sum()
            children.append(child)
        population = np.vstack((parents, children))

    # Return the individual with highest fitness
    fitness = np.array([sharpe_ratio(ind, returns) for ind in population])
    best_idx = fitness.argmax()
    return population[best_idx]


def backtest_portfolio(symbols: Sequence[str]) -> BacktestResult:
    """Backtest a portfolio of symbols and return the result."""
    prices = fetch_prices(symbols)
    returns = prices.pct_change().dropna()

    split = int(len(returns) * 0.8)
    train = returns.iloc[:split]
    test = returns.iloc[split:]

    weights = evolutionary_optimize(train)
    train_score = sharpe_ratio(weights, train)
    test_score = sharpe_ratio(weights, test)

    return BacktestResult(symbols, weights, train_score, test_score)


def optimise_portfolios(portfolios: Iterable[Sequence[str]]) -> BacktestResult:
    """Evaluate multiple portfolios and return the best one."""
    results = [backtest_portfolio(p) for p in portfolios]
    return max(results, key=lambda r: r.test_sharpe)


if __name__ == "__main__":
    # Example usage with two candidate portfolios
    candidate_portfolios: List[List[str]] = [
        ["510300", "510500"],
        ["159915", "159949", "159922"],
    ]
    best = optimise_portfolios(candidate_portfolios)
    print("Best portfolio based on test Sharpe ratio:")
    print(f"Symbols: {best.symbols}")
    print(f"Weights: {np.round(best.weights, 3)}")
    print(f"Train Sharpe: {best.train_sharpe:.3f}")
    print(f"Test Sharpe: {best.test_sharpe:.3f}")
