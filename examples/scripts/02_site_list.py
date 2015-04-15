"""
Graph manager example with site list
"""
from frontera import graphs

SITE_LIST = [
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

if __name__ == '__main__':
    # Create graph
    graph = graphs.Manager()

    # Add site list to graph
    graph.add_site_list(SITE_LIST)

    # Show graph pages
    for page in graph.pages:
        print page
