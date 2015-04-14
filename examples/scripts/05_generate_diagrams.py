"""
Graph diagram generation example
"""
from frontera import graphs

SITE_LIST_A = [
    [
        ("A1", ["A2", "A3"]),
        ("A2", ["A4", "A5"]),
        ("A3", ["A6", "A7"]),
    ],
    [
        ("B1", ["B2", "B3"]),
        ("B2", ["B4", "B5"]),
        ("B3", ["B6", "B7"]),
    ],
]

SITE_LIST_B = [
    [
        ("http://www.google.com", ["http://www.yahoo.com", "http://scrapinghub.com"]),
    ],
]


def generate_graph(site_list, filename, title, use_urls=False):
    print 'Generating diagram "%s"...' % title
    graph = graphs.Manager()
    graph.add_site_list(site_list)
    graph.render(filename=filename, label=title, use_urls=use_urls)

if __name__ == '__main__':
    generate_graph(SITE_LIST_A, 'diagrams/A.png', 'Example Graph A')
    generate_graph(SITE_LIST_B, 'diagrams/B.png', 'Example Graph B', True)