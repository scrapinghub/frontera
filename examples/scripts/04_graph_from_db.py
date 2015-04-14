"""
Graph manager reading data from database
"""
from frontera import graphs

if __name__ == '__main__':
    # Create graph with sqlite db
    graph = graphs.Manager('sqlite:///data/graph.db')

    # Show graph pages
    for page in graph.pages:
        print page

