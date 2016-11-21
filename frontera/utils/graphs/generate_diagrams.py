from __future__ import absolute_import
from __future__ import print_function
import os
import sys

from .manager import CrawlGraphManager
from .data import GRAPHS

SCRIPT_FOLDER = os.path.abspath(os.path.split(sys.argv[0])[0])
CHARTS_FOLDER = os.path.join(SCRIPT_FOLDER, 'diagrams')


def generate_filename(graph_name):
    name = graph_name
    name = name.replace(' ', '_')
    name = name.lower()
    name = '%s.png' % name
    return name


def generate_graph_diagram(filename, title, graph):
    print("generating png diagram for test '%s'..." % title)
    manager = CrawlGraphManager()
    manager.add_site_list(graph)
    manager.render(filename, label=title, use_urls=graph.use_urls)


def generate_diagrams():
    for graph in GRAPHS:
        generate_graph_diagram(filename=os.path.join(CHARTS_FOLDER, generate_filename(graph.name)),
                               title=graph.name,
                               graph=graph)


if __name__ == '__main__':
    generate_diagrams()














