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
        self._n_pages = self._scores.get_count()

        # Total hub history, excluding virtual page
        # adding a small quantity is easier and faster than checking 0/0
        self._h_total = 1e-6 
        
        # Total authority history, excluding virtual page
        # adding a small quantity is easier and faster than checking 0/0
        self._a_total = 1e-6 

        # A list of pages to update
        self._to_update = []

        # A virtual page connected from and to every
        # other web page
        self._virtual_page = hitsdb.HitsScore(
            h_history = 0.0,
            h_cash    = 1.0,
            a_history = 0.0,
            a_cash    = 1.0
        )

        self._closed = False

        # Initialize scores
        for page_id in self._graph.inodes():
            self.add_page(page_id)

    def mark_update(self, page_id):
        """Add this to the list of pages to update"""
        self._to_update.append(page_id)

    def add_page(self, page_id):
        """Add a new page, with fresh score information"""
        if not page_id in self._scores:
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
        else:
            new_score = None

        return new_score

    def _get_page_score(self, page_id):
        """Return HITS score information. If page has not been 
        associated yet it will create a new association
        """
      
        score = self.add_page(page_id)
        if not score:
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
        self._scores.increase_a_cash(succ, a_dist)
        # Give hub score to predecessors
        self._scores.increase_h_cash(pred, h_dist)

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

        self._scores.increase_all_cash(h_dist, a_dist)
        
        self._virtual_page.h_history += self._virtual_page.h_cash
        self._virtual_page.a_history += self._virtual_page.a_cash

        self._virtual_page.h_cash = 0.0
        self._virtual_page.a_cash = 0.0

    def update(self, n_iter=1):
        """Run a full iteration of the
        OPIC-HITS algorithm"""

        # update proportional to the rate of graph grow
        n_updates = max(1, len(self._to_update))

        for i in xrange(n_iter):                       
            best_h, h_cash = zip(*self._scores.get_highest_h_cash(n_updates))
            best_a, a_cash = zip(*self._scores.get_highest_a_cash(n_updates))
            
            best = set(best_h + best_a + tuple(self._to_update))            

            if (self._virtual_page.h_cash > h_cash[-1] or
                self._virtual_page.a_cash > a_cash[-1]):                                
                self.update_virtual_page()
           
            for page_id in best:
                self.update_page(page_id)

        self._to_update = []

        return best

    def get_scores(self, page_id):
        """Return a tuple (hub score, authority score) for the given 
        page_id"""

        score = self._get_page_score(page_id)
        return (score.h_history/self._h_total,
                score.a_history/self._a_total)

    def iscores(self):
        """Iterate over (page_id, hub score, authority score)"""
        for page_id, score in self._scores.iteritems():
            yield (page_id,
                   score.h_history/self._h_total,
                   score.a_history/self._a_total)
        
    def close(self):
        if not self._closed:
            self._graph.close()
            self._scores.close()

        self._closed = True
