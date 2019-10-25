#!/usr/bin/env python3

from __future__ import absolute_import
from __future__ import print_function
import os
import sys

import six

from frontera.utils.graphs.manager import CrawlGraphManager
from frontera.utils.graphs.data import GRAPHS

if six.PY2:
    from urlparse import urljoin
elif six.PY3:
    from urllib.parse import urljoin

CHARTS_FOLDER = urljoin(__file__, "diagrams/")


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


def generate_diagrams(outdir=CHARTS_FOLDER):
    for graph in GRAPHS:
        generate_graph_diagram(filename=os.path.join(outdir, generate_filename(graph.name)),
                               title=graph.name,
                               graph=graph)


if __name__ == '__main__':
    generate_diagrams()
