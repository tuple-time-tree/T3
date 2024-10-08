import json

from matplotlib import pyplot as plt

from src.figures.infra import setup_matplotlib_latex_font, get_figure_path, get_hex_color, get_figure_format


def latency_scaling_figure():
    setup_matplotlib_latex_font()

    with open("dp/latencyScaling.json", "r") as file:
        data = json.load(file)

    x = [float(k) for k in data.keys()]
    y = list(data.values())

    fig, ax = plt.subplots(1, 1, figsize=(6, 3))

    # mark avg query size
    avg_query_size = 3
    # print(f"avg query has {avg_query_size} pipelines and {data[str(avg_query_size)]} latency")
    ax.scatter(avg_query_size, y[avg_query_size - 1], color=get_hex_color("my_red"))
    ax.annotate(
        "Avg Query",
        (avg_query_size, y[avg_query_size - 1]),
        textcoords="offset points",
        xytext=(0, 5),
        ha="center",
        fontsize=8,
        # color="grey"
    )

    ax.plot(x, y, color=get_hex_color("my_blue"))

    ax.set_ylim(-0.05, 0.80)

    # naming
    label = ax.set_ylabel("Latency in ms")
    fig.supxlabel("Number of Predicted Pipelines", color=label._color, font=label._fontproperties, y=0.1)

    fig.savefig(f"{get_figure_path()}/latency_scaling.{get_figure_format()}", bbox_inches="tight")


def main():
    latency_scaling_figure()


if __name__ == "__main__":
    main()
