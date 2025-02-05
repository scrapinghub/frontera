import sys
from pathlib import Path

from .data import GRAPHS
from .manager import CrawlGraphManager

SCRIPT_FOLDER = Path(sys.argv[0]).parent.absolute()
CHARTS_FOLDER = SCRIPT_FOLDER / "diagrams"


def generate_filename(graph_name):
    name = graph_name
    name = name.replace(" ", "_")
    name = name.lower()
    return f"{name}.png"


def generate_graph_diagram(filename, title, graph):
    print(f"generating png diagram for test '{title}'...")
    manager = CrawlGraphManager()
    manager.add_site_list(graph)
    manager.render(filename, label=title, use_urls=graph.use_urls)


def generate_diagrams():
    for graph in GRAPHS:
        generate_graph_diagram(
            filename=CHARTS_FOLDER / generate_filename(graph.name),
            title=graph.name,
            graph=graph,
        )


if __name__ == "__main__":
    generate_diagrams()
