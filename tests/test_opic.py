import crawlfrontier.contrib.backends.opic.graphdb as graphdb
import crawlfrontier.contrib.backends.opic.hitsdb as hitsdb
import crawlfrontier.contrib.backends.opic.pagedb as pagedb
import crawlfrontier.contrib.backends.opic.scoredb as scoredb
import crawlfrontier.contrib.backends.opic.pagechange as pagechange
import crawlfrontier.contrib.backends.opic.hashdb as hashdb

from crawlfrontier.contrib.backends.opic.opichits import OpicHits

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
    db.add('a', hitsdb.HitsScore(1, 2, 3, 4))
    db.add('b', hitsdb.HitsScore(5, 5, 5, 5))
    db.add('c', hitsdb.HitsScore(9, 8, 7, 6))

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

    db.set('b', hitsdb.HitsScore(-1, -2, -3, -4))
    b_get = db.get('b')    

    assert b_get.h_history == -1
    assert b_get.h_cash == -2
    assert b_get.a_history == -3
    assert b_get.a_cash == -4

    db.delete('a')
    assert db.get('a') == None

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
    assert db.get('b') == None

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
    opic.update(n_iter=10)

    h_score, a_score = zip(
        *[opic.get_scores(page_id) 
          for page_id in ['0', '1', '2', '3', '4']]
    )

    assert h_score[0] >= 0.25 and h_score[0] <= 0.3
    assert a_score[0] >= 0.25 and a_score[0] <= 0.3

    for s in h_score[1:]:
        assert s >= 0.15 and s<= 0.2
    for s in a_score[1:]:
        assert s >= 0.15 and s<= 0.2

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


