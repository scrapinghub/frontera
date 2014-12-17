import shutil
import tempfile

from collections import defaultdict

from crawlfrontier import FrontierManager, Settings
from crawlfrontier.core.models import Request, Response


import crawlfrontier.contrib.backends.opic.graphdb as graphdb
import crawlfrontier.contrib.backends.opic.hitsdb as hitsdb
import crawlfrontier.contrib.backends.opic.pagedb as pagedb
import crawlfrontier.contrib.backends.opic.scoredb as scoredb
import crawlfrontier.contrib.backends.opic.pagechange as pagechange
import crawlfrontier.contrib.backends.opic.hashdb as hashdb
import crawlfrontier.contrib.backends.opic.freqdb as freqdb
import crawlfrontier.contrib.backends.opic.linksdb as linksdb

from crawlfrontier.contrib.backends.opic.opichits import OpicHits
from crawlfrontier.contrib.backends.opic.backend import OpicHitsBackend


def freq_counter(iterable):
    """Count how many times each element is repeated inside the iterable

    Returns: a dictionary mapping each element to its count
    """
    freqs = defaultdict(int)
    for i in iterable:
        freqs[i] += 1

    return freqs


def create_test_graph_1(g):
    g.clear()

    g.add_node('a')
    g.add_node('b')
    g.add_node('c')
    g.add_node('d')

    g.add_edge('a', 'b')
    g.add_edge('a', 'c')
    g.add_edge('b', 'd')
    g.add_edge('c', 'd')

    return g


def create_test_graph_2(g):
    g.clear()

    g.add_node('0')
    g.add_node('1')
    g.add_node('2')
    g.add_node('3')
    g.add_node('4')

    g.add_edge('0', '1')
    g.add_edge('0', '2')
    g.add_edge('0', '3')
    g.add_edge('0', '4')

    g.add_edge('1', '0')
    g.add_edge('1', '2')
    g.add_edge('2', '0')
    g.add_edge('2', '3')
    g.add_edge('3', '0')
    g.add_edge('3', '4')
    g.add_edge('4', '0')
    g.add_edge('4', '1')

    return g


def _test_graph_db(g):
    g = create_test_graph_1(g)

    assert g.has_node('a')
    assert g.has_node('b')
    assert g.has_node('c')
    assert g.has_node('d')

    assert not g.has_node('x')

    assert set(g.inodes()) == set(['a', 'b', 'c', 'd'])
    assert set(g.iedges()) == set([
        ('a', 'b'),
        ('a', 'c'),
        ('b', 'd'),
        ('c', 'd')
    ])

    assert set(g.successors('a')) == set(['b', 'c'])
    assert set(g.successors('b')) == set(['d'])
    assert set(g.successors('c')) == set(['d'])
    assert set(g.successors('d')) == set([])

    assert set(g.predecessors('a')) == set([])
    assert set(g.predecessors('b')) == set(['a'])
    assert set(g.predecessors('c')) == set(['a'])
    assert set(g.predecessors('d')) == set(['b', 'c'])

    g.delete_node('b')
    assert set(g.successors('a')) == set(['c'])
    assert set(g.predecessors('d')) == set(['c'])


def test_graph_lite_db():
    g = graphdb.SQLite()
    g.clear()

    _test_graph_db(g)

    g.close()


def _test_hits_db(db):
    db.add('a', hitsdb.HitsScore(1, 2, 0, 3, 4, 0))
    db.add('b', hitsdb.HitsScore(5, 5, 0, 5, 5, 0))
    db.add('c', hitsdb.HitsScore(9, 8, 0, 7, 6, 0))

    a_get = db.get('a')
    b_get = db.get('b')
    c_get = db.get('c')

    assert a_get.h_history == 1
    assert a_get.h_cash == 2
    assert a_get.a_history == 3
    assert a_get.a_cash == 4

    assert b_get.h_history == 5
    assert b_get.h_cash == 5
    assert b_get.a_history == 5
    assert b_get.a_cash == 5

    assert c_get.h_history == 9
    assert c_get.h_cash == 8
    assert c_get.a_history == 7
    assert c_get.a_cash == 6

    assert 'a' in db
    assert 'b' in db
    assert 'c' in db
    assert 'x' not in db

    db.set('b', hitsdb.HitsScore(-1, -2, 0, -3, -4, 0))
    b_get = db.get('b')

    assert b_get.h_history == -1
    assert b_get.h_cash == -2
    assert b_get.a_history == -3
    assert b_get.a_cash == -4

    db.delete('a')
    assert db.get('a') is None

    db.add('0', hitsdb.HitsScore(0, 0.1, 0, 0, 0.2, 0))
    db.add('1', hitsdb.HitsScore(0, 1.1, 0, 0, 1.2, 0))
    db.add('2', hitsdb.HitsScore(0, 2.1, 0, 0, 2.2, 0))

    db.increase_h_cash(['0', '1', '2'], 0.5)
    db.increase_a_cash(['0', '1', '2'], 0.5)

    assert abs(db.get('0').h_cash - 0.6) < 1e-6
    assert abs(db.get('0').a_cash - 0.7) < 1e-6
    assert abs(db.get('1').h_cash - 1.6) < 1e-6
    assert abs(db.get('1').a_cash - 1.7) < 1e-6
    assert abs(db.get('2').h_cash - 2.6) < 1e-6
    assert abs(db.get('2').a_cash - 2.7) < 1e-6

    db.increase_all_cash(1.0, 2.0)

    assert abs(db.get('0').h_cash - 1.6) < 1e-6
    assert abs(db.get('0').a_cash - 2.7) < 1e-6
    assert abs(db.get('1').h_cash - 2.6) < 1e-6
    assert abs(db.get('1').a_cash - 3.7) < 1e-6
    assert abs(db.get('2').h_cash - 3.6) < 1e-6
    assert abs(db.get('2').a_cash - 4.7) < 1e-6

    db.set('0', hitsdb.HitsScore(1, 2, 1, 3, 1, 4))

    zero_get = db.get('0')
    assert zero_get.h_history == 1.0
    assert zero_get.h_cash == 2.0
    assert zero_get.h_last == 1.0
    assert zero_get.a_history == 3.0
    assert zero_get.a_cash == 1.0
    assert zero_get.a_last == 4.0

    db.increase_h_cash(['0', '1', '2'], 0.1)
    db.increase_a_cash(['0', '1', '2'], 0.1)

    assert abs(db.get('0').h_cash - 2.1) < 1e-6
    assert abs(db.get('0').a_cash - 1.1) < 1e-6
    assert abs(db.get('1').h_cash - 2.7) < 1e-6
    assert abs(db.get('1').a_cash - 3.8) < 1e-6
    assert abs(db.get('2').h_cash - 3.7) < 1e-6
    assert abs(db.get('2').a_cash - 4.8) < 1e-6

    assert db.get_count() == 5


def test_hits_lite_db():
    db = hitsdb.SQLite()
    db.clear()

    _test_hits_db(db)

    db.clear()
    db.close()


def _test_page_db(db):
    db.add('a', pagedb.PageData(url='foo', domain='bar'))
    db.add('b', pagedb.PageData(url='spam', domain='eggs'))

    a_get = db.get('a')
    b_get = db.get('b')

    assert a_get.url == 'foo'
    assert a_get.domain == 'bar'
    assert b_get.url == 'spam'
    assert b_get.domain == 'eggs'

    db.set('a', pagedb.PageData(url='unladen', domain='swallow'))
    a_get = db.get('a')

    assert a_get.url == 'unladen'
    assert a_get.domain == 'swallow'

    db.delete('b')
    assert db.get('b') is None


def test_page_lite_db():
    db = pagedb.SQLite()
    db.clear()

    _test_page_db(db)

    db.clear()
    db.close()


def _test_score_db(db):
    db.add('a', 1)
    db.add('b', 2)
    db.add('c', 3)
    db.add('d', 4)
    db.add('e', 3)
    db.add('f', 2)
    db.add('g', 1)

    assert 1 == db.get('a')
    assert 2 == db.get('b')
    assert 3 == db.get('c')
    assert 4 == db.get('d')
    assert 3 == db.get('e')
    assert 2 == db.get('f')
    assert 1 == db.get('g')

    db.set('c', 100)

    assert 100 == db.get('c')

    best = db.get_best_scores(2)

    assert best[0] == ('c', 100)
    assert best[1] == ('d', 4)

    db.delete('c')
    assert db.get('c') == 0.0


def test_score_lite_db():
    db = scoredb.SQLite()
    db.clear()

    _test_score_db(db)

    db.clear()
    db.close()


def test_opic():
    g = graphdb.SQLite()
    g.clear()

    h = hitsdb.SQLite()
    h.clear()

    opic = OpicHits(db_graph=create_test_graph_2(g), db_scores=h)
    opic.update(n_iter=100)

    h_score, a_score = zip(
        *[opic.get_scores(page_id)
          for page_id in ['0', '1', '2', '3', '4']]
    )

    assert h_score[0] >= 0.25 and h_score[0] <= 0.3
    assert a_score[0] >= 0.25 and a_score[0] <= 0.3

    for s in h_score[1:]:
        assert s >= 0.15 and s <= 0.2
    for s in a_score[1:]:
        assert s >= 0.15 and s <= 0.2

    g.close()
    h.close()


def _test_pagechange(db):
    assert db.update('a', '123')
    assert db.update('b', 'aaa')
    assert not db.update('b', 'aaa')
    assert not db.update('a', '123')
    assert db.update('a', '120')


def test_pagechange_sha1():
    db = hashdb.SQLite()
    db.clear()

    _test_pagechange(pagechange.BodySHA1(db))

    db.clear()
    db.close()


def _test_freq(db):

    db.add('0', 1.0)
    db.add('1', 1.0)
    db.add('2', 4.0)
    db.add('3', 8.0)
    db.add('4', 8.0)
    db.add('5', 1.0)
    db.add('6', 100.0)

    db.set('5', 8.5)
    db.delete('6')

    N = 1000
    pages = []
    for i in xrange(N):
        pages += db.get_next_pages()
    freq = freq_counter(pages)

    def check_eps(x, a, eps=1e-1):
        return (a - eps <= x) and (x <= a + eps)

    assert freq['0'] > 0
    assert check_eps(freq['1'], freq['0'], N*0.05)
    assert check_eps(freq['2'], 4.0 * freq['0'], N*0.05)
    assert check_eps(freq['3'], 8.0 * freq['0'], N*0.05)
    assert check_eps(freq['4'], 8.0 * freq['0'], N*0.05)
    assert check_eps(freq['5'], 8.5 * freq['0'], N*0.05)
    assert check_eps(freq['6'], 0)


def test_freq_lite_db():
    db = freqdb.SQLite()
    db.clear()

    _test_freq(db)

    db.clear()
    db.close()


def _test_links(db):
    db.add('a', 'b', 1, 2)
    db.add('a', 'c', 0, 0)
    db.add('a', 'd', 3, 1)
    db.add('b', 'a', 5, 5)
    db.add('b', 'd', 8, 9)
    db.add('b', 'c', 8, 9)

    db.delete('b', 'c')

    db.set('b', 'd', 0, 0)

    assert db.get('a', 'b') == (1, 2)
    assert db.get('a', 'c') == (0, 0)
    assert db.get('a', 'd') == (3, 1)
    assert db.get('b', 'a') == (5, 5)
    assert db.get('b', 'c') is None
    assert db.get('b', 'd') == (0, 0)


def test_links_lite_db():
    db = linksdb.SQLite()
    db.clear()

    _test_links(db)

    db.clear()
    db.close()


def test_stop_resume():
    def simple_request(url):
        r = Request(url)
        r.meta['fingerprint'] = url
        r.meta['domain'] = {'name': ''}

        return r

    def simple_response(url):
        return Response(url, request=simple_request(url))

    workdir = tempfile.mkdtemp()
    settings = Settings(None,
                        attributes={
                            'BACKEND':
                            'crawlfrontier.contrib.backends.opic.backend.OpicHitsBackend',
                            'BACKEND_OPIC_IN_MEMORY': False,
                            'BACKEND_OPIC_WORKDIR': workdir,
                            'BACKEND_MIN_NEXT_PAGES': 1,
                            'LOGGING_EVENTS_ENABLED': False,
                            'BACKEND_DOMAIN_DEPTH': None,
                            'MAX_REQUESTS': 100,
                            'BACKEND_MIN_NEXT_PAGES': 1,
                            'MAX_NEXT_REQUESTS': 1
                        })

    frontier = FrontierManager.from_settings(settings)

    # First crawl
    # -----------------------------------
    crawled1 = []
    backend1 = OpicHitsBackend.from_manager(frontier)
    backend1.frontier_start()

    seeds = [
        simple_request('A'),
        simple_request('B'),
    ]

    backend1.add_seeds(seeds)

    crawled1 += backend1.get_next_requests(1)
    crawled1 += backend1.get_next_requests(1)

    backend1.page_crawled(
        simple_response('A'),
        [
            simple_response('1'),
            simple_response('2'),
            simple_response('3')
        ]
    )

    backend1.page_crawled(
        simple_response('B'),
        [
            simple_response('4'),
            simple_response('5'),
            simple_response('6')
        ]
    )

    crawled1 += backend1.get_next_requests(1)
    crawled1 += backend1.get_next_requests(1)

    backend1.frontier_stop()

    # Second crawl
    # -----------------------------------
    backend2 = OpicHitsBackend.from_manager(frontier)
    backend2.frontier_start()

    crawled2 = []
    for i in xrange(100):
        requests = backend2.get_next_requests(1)
        crawled2 += requests
        for request in requests:
            backend2.page_crawled(
                simple_response(request.url),
                []
            )

    backend2.frontier_stop()

    # Clean temp files
    shutil.rmtree(workdir)

    crawled1 = set([r.url for r in crawled1])
    crawled2 = set([r.url for r in crawled2])

    assert (crawled1 | crawled2 ==
            set(['A', 'B', '1', '2', '3', '4', '5', '6']))

    assert crawled1 & crawled2 == set([])
