import math

import numpy as np
from matplotlib import pyplot as plt

from src.data_collection import DataCollector
from src.database_manager import DatabaseManager
from src.evaluation import QueryEstimationCache
from src.figures.infra import setup_matplotlib_latex_font, get_figure_path, get_figure_format
from src.metrics import q_error
from src.train import optimize_all


def get_error_histogram(estimation_cache: QueryEstimationCache):
    setup_matplotlib_latex_font()

    dbs = DatabaseManager.get_test_databases()
    benchmarks = DataCollector.collect_benchmarks(dbs, False)
    runtimes = [b.get_total_runtime() for b in benchmarks]

    estimates = [estimation_cache.queries[q.name].estimated_time for q in benchmarks]
    q_errors = [q_error(e, r) for e, r in zip(estimates, runtimes)]
    data = np.array(q_errors)
    n_bins = 2 * 12 + 1
    bin_edges = np.linspace(math.floor(data.min()), math.ceil(data.max()), num=n_bins)
    width = math.ceil(data.max()) - math.floor(data.min())
    bin_width = width / n_bins

    # Calculate histogram with logarithmic bins
    hist, bins = np.histogram(data, bins=bin_edges)

    # Plot the histogram
    fig = plt.figure(figsize=(6, 3))
    plt.hist(bins[:-1], bins, weights=hist, edgecolor="black")

    # Plot numbers on bars
    for bin_boundary, freq in zip(bins, hist):
        if 10 > freq > 0:
            plt.annotate(
                f"{freq:.0f}",
                xy=(bin_boundary + bin_width / 2, freq),
                xycoords="data",
                xytext=(0, 2),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
            )

    # naming
    fig.get_axes()[0].set_xlim(left=0)
    plt.xlabel("Q-Error of Test Queries")
    plt.ylabel("Frequency")

    plt.savefig(f"{get_figure_path()}/test_accuracy_histogram.{get_figure_format()}", bbox_inches="tight")


def main():
    model = optimize_all()
    estimation_cache = QueryEstimationCache(model, False)
    get_error_histogram(estimation_cache)


if __name__ == "__main__":
    main()
