"""
An implementation of the OPIC-HITS algorithm
"""
import hitsdb
import graphdb


class OpicHits(object):
    def __init__(self, db_graph=None, db_scores=None):
        """Make a new instance, using db_graph as the graph database and 
        db_scores as the HITS scores database. If None provided will create 
        SQLite in-memory instances
        """
        # Web page connectivity information
        self._graph = db_graph or graphdb.SQLite()

        # HITS score associated to each web page
        self._scores = db_scores or hitsdb.SQLite()
  
        # Number of scored web pages
        self._n_pages = 0

        # Total hub history, excluding virtual page
        # adding a small quantity is easier and faster than checking 0/0
        self._h_total = 1e-6 
        
        # Total authority history, excluding virtual page
        # adding a small quantity is easier and faster than checking 0/0
        self._a_total = 1e-6 

        # A virtual page connected from and to every
        # other web page
        self._virtual_page = hitsdb.HitsScore(
            h_history = 0.0,
            h_cash    = 1.0,
            a_history = 0.0,
            a_cash    = 1.0
        )

    def _add_new_page(self, page_id):
        """Add a new page, with fresh score information"""
        self._n_pages += 1

        new_score = hitsdb.HitsScore(
            h_history = self._h_total/self._n_pages,
            h_cash    = 0.0,
            a_history = self._a_total/self._n_pages,
            a_cash    = 0.0            
        )

        self._scores.add(page_id, new_score)

        self._h_total += new_score.h_history
        self._a_total += new_score.a_history

        return new_score

    def _get_page_score(self, page_id):
        """Return HITS score information. If page has not been 
        associated yet it will create a new association
        """
        if not page_id in self._scores:
            score = self._add_new_page(page_id)
        else:
            score = self._scores.get(page_id)

        return score

    def update_page(self, page_id):
        """Update HITS score for the given page"""
        score = self._get_page_score(page_id)

        # Add cash to total cash count
        self._h_total += score.h_cash
        self._a_total += score.a_cash

        # Distribute cash to neighbours and VP
        succ = self._graph.successors(page_id)        
        pred = self._graph.predecessors(page_id)

        # Authority cash gets distributed to hubs
        h_dist = score.a_cash/(len(pred) + 1.0)
        # Hub cash gets distributed to authorities
        a_dist = score.h_cash/(len(succ) + 1.0)

        # Give authority to successors
        for s_page_id in succ:
            s_page_score = self._get_page_score(s_page_id)
            s_page_score.a_cash += a_dist

            self._scores.set(s_page_id, s_page_score)

        # Give hub score to predecessors
        for p_page_id in pred:
            p_page_score = self._get_page_score(p_page_id)
            p_page_score.h_cash += h_dist

            self._scores.set(p_page_id, p_page_score)

        self._virtual_page.h_cash += h_dist
        self._virtual_page.a_cash += a_dist

        # Update own-score info
        score.h_history += score.h_cash
        score.h_cash = 0
        score.a_history += score.a_cash
        score.a_cash = 0
        
        self._scores.set(page_id, score)        

    def update_virtual_page(self):
        """Repeat update_page, but on the virtual page"""

        h_dist = self._virtual_page.a_cash/self._n_pages
        a_dist = self._virtual_page.h_cash/self._n_pages

        for page_id in self._graph.inodes():
            score = self._scores.get(page_id)
            score.h_cash += h_dist
            score.a_cash += a_dist

            self._scores.set(page_id, score)

        self._virtual_page.h_history += self._virtual_page.h_cash
        self._virtual_page.a_history += self._virtual_page.a_cash

        self._virtual_page.h_cash = 0.0
        self._virtual_page.a_cash = 0.0        

    def update(self, n_iter=1):
        """Run a full iteration of the OPIC-HITS algorithm"""
        for i in xrange(n_iter):           
            for page_id in self._graph.inodes():
                self.update_page(page_id)

            self.update_virtual_page()

    def get_scores(self, page_id):
        """Return a tuple (hub score, authority score) for the given 
        page_id"""

        score = self._scores.get(page_id)
        return (score.h_history/self._h_total,
                score.a_history/self._a_total)

    def iscores(self):
        """Iterate over (page_id, hub score, authority score)"""
        for page_id, score in self._scores.iteritems():
            yield (page_id,
                   score.h_history/self._h_total,
                   score.a_history/self._a_total)
        

