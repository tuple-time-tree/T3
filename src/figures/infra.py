from pathlib import Path

import matplotlib
from cycler import Cycler, cycler
from matplotlib import pyplot as plt

FIGURE_PATH = "./figure_output"
FIGURE_FORMAT = "pdf"
USE_LATEX = False


def get_colors() -> dict:
    colors = {
        "my_blue": "3274B4",
        "my_teal": "4CA990",
        "my_green": "53AE33",
        "my_yellow": "F1B13D",
        "my_red": "DB3B26",
        "my_pink": "C13175",
        "my_light_blue": "72BFF9",
        "my_light_teal": "97FAE9",
        "my_light_green": "A4F76B",
        "my_light_yellow": "FDF171",
        "my_light_red": "F19B90",
        "my_light_pink": "F09AC9",
    }
    return colors


def get_hex_color(name: str) -> str:
    return f"#{get_colors()[name]}"


def get_hex_colors(names: list[str]) -> list[str]:
    return [get_hex_color(name) for name in names]


def make_cycle(color_cycle: list) -> Cycler:
    color_list = [f"#{get_colors()[c]}" for c in color_cycle]
    return cycler(color=color_list)


def setup_matplotlib_latex_font():
    color_cycle = ["my_blue", "my_red", "my_green", "my_yellow", "my_pink", "my_teal"]
    light_color_cycle = [
        "my_light_blue",
        "my_light_red",
        "my_light_green",
        "my_light_yellow",
        "my_light_pink",
        "my_light_teal",
    ]
    plt.style.use("ggplot")
    rc_params = {
        "axes.prop_cycle": make_cycle(color_cycle),
        "figure.autolayout": True,
        "font.family": "serif",
        "font.size": 14,
        "axes.facecolor": "white",
        "grid.color": "E5E5E5",
        "axes.edgecolor": "E5E5E5",
        "axes.labelcolor": "black",
        "xtick.color": "black",
        "ytick.color": "black",
        # "grid.alpha": 0.2,
        # "axes.facecolor": "F5F5F5",
    }
    if get_use_latex():
        rc_params.update(
            {
                "text.usetex": True,
                "text.latex.preamble": r"""
            \usepackage{libertine}
            \usepackage[libertine]{newtxmath}
            """,
            }
        )
    matplotlib.rcParams.update(rc_params)
    plt.tight_layout()
    return color_cycle, light_color_cycle


def get_figure_path() -> Path:
    global FIGURE_PATH
    return Path(FIGURE_PATH)


def set_figure_path(path: str):
    global FIGURE_PATH
    FIGURE_PATH = path


def get_figure_format() -> str:
    global FIGURE_FORMAT
    return FIGURE_FORMAT


def set_figure_format(figure_format: str):
    global FIGURE_FORMAT
    FIGURE_FORMAT = figure_format


def get_use_latex() -> bool:
    global USE_LATEX
    return USE_LATEX


def set_use_latex(use_latex: bool):
    global USE_LATEX
    USE_LATEX = use_latex


def write_latex_file(text: str, name: str):
    with open(f"{get_figure_path()}/{name}.tex", "w") as file:
        file.write(text)
