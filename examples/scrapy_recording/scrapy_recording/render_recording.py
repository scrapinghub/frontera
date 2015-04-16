from frontera import graphs

graph = graphs.Manager('sqlite:///recordings/record.db')
graph.render(filename='recordings/record.png', label='Record graph', use_urls=True, include_ids=True)

