"""
Graph manager example with single site
"""
from frontera import graphs

SITE = [
    ("A", ["B", "C"]),
    ("B", ["D", "E"]),
    ("C", ["F", "G"]),
]

SITE_WITH_STATUS_CODES = [
    ((200, "A"), ["B", "C"]),
    ((404, "B"), ["D", "E"]),
    ((500, "C"), ["F", "G"]),
]


def test_site(site):
    # Create graph
    graph = graphs.Manager()

    # Add site to graph
    graph.add_site(site)

    # Show graph pages
    print '-'*80
    for page in graph.pages:
        print page, page.status

    # Show single page
    a_page = graph.get_page("A")
    print a_page.url, [link.url for link in a_page.links]


if __name__ == '__main__':
    test_site(SITE)
    test_site(SITE_WITH_STATUS_CODES)