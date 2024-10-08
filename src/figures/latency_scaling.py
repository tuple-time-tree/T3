import json

from matplotlib import pyplot as plt
from matplotlib.patches import ConnectionPatch

from src.figures.infra import setup_matplotlib_latex_font, get_figure_path, get_hex_color, get_figure_format


def latency_scaling_figure():
    setup_matplotlib_latex_font()

    with open("dp/latencyScaling.json", "r") as file:
        data = json.load(file)

    x = [float(k) for k in data.keys()]
    y = list(data.values())

    fig, ax = plt.subplots(1, 2, figsize=(6, 3))

    # mark avg query size
    avg_query_size = 3
    # print(f"avg query has {avg_query_size} pipelines and {data[str(avg_query_size)]} latency")
    for i in (0, 1):
        ax[i].scatter(avg_query_size, y[avg_query_size - 1], color=get_hex_color("my_red"))
        ax[i].annotate(
            "Avg Query",
            (avg_query_size, y[avg_query_size - 1]),
            textcoords="offset points",
            xytext=(5, 0),
            ha="left",
            fontsize=8,
            # color="grey"
        )

    ax[0].plot(x[:100], y[:100], color=get_hex_color("my_blue"))
    ax[1].plot(x, y, color=get_hex_color("my_blue"))

    # ticks
    ax[0].set_xticks([0, 25, 50, 75, 100])
    ax[0].set_yticks([0, 0.025, 0.050, 0.075, 0.100])

    # naming
    label = ax[0].set_ylabel("Latency in ms")
    fig.supxlabel("Number of Predicted Pipelines", color=label._color, font=label._fontproperties, y=0.1)

    # Add a rectangle to the right plot to represent the zoomed-in range
    linewidth = 1.5
    linestyle = (0, (2, 3))
    alpha = 0.45
    rect_x_start = 0
    rect_x_end = x[99]
    rect_y_min = 0
    rect_y_max = max(y[:100])
    rect_width = rect_x_end - rect_x_start
    rect_height = rect_y_max - rect_y_min

    rectangle = plt.Rectangle(
        (rect_x_start, rect_y_min),
        rect_width,
        rect_height,
        linewidth=linewidth,
        edgecolor=label._color,
        facecolor="none",
        linestyle=linestyle,
        capstyle="round",
        alpha=alpha,
    )
    ax[1].add_patch(rectangle)

    ax[0].set_xlim(rect_x_start, rect_x_end)
    ax[0].set_ylim(rect_y_min, 0.1)

    con = ConnectionPatch(
        xyA=(rect_x_end, rect_y_max),
        xyB=(rect_x_start, rect_y_max),
        coordsA="data",
        coordsB="data",
        axesA=ax[0],
        axesB=ax[1],
        color=label._color,
        linewidth=linewidth,
        linestyle=linestyle,
        alpha=alpha,
    )
    fig.add_artist(con)

    con2 = ConnectionPatch(
        xyA=(rect_x_end, rect_y_min),
        xyB=(rect_x_start, rect_y_min),
        coordsA="data",
        coordsB="data",
        axesA=ax[0],
        axesB=ax[1],
        color=label._color,
        linewidth=linewidth,
        linestyle=linestyle,
        alpha=alpha,
    )
    fig.add_artist(con2)

    fig.savefig(f"{get_figure_path()}/latency_scaling.{get_figure_format()}", bbox_inches="tight")


def main():
    latency_scaling_figure()


if __name__ == "__main__":
    main()
