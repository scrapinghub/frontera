import os

from frontera.utils.graphs.generate_diagrams import generate_diagrams


def test_generate_diagrams(tmpdir):
    """Generate some example graph visualizations."""
    generate_diagrams(outdir=tmpdir)
    for idx in range(1, 10):
        assert os.path.isfile(tmpdir / "graph_0{}.png".format(idx))
