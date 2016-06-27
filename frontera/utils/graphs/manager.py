from __future__ import absolute_import
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Base, CrawlPage
from .data import CrawlSiteData, CrawlSiteListData

DEFAULT_ENGINE = 'sqlite:///:memory:'


class CrawlGraphManager(object):
    def __init__(self, engine=DEFAULT_ENGINE, autocommit=False, autoflush=False,
                 echo=False, drop_all_tables=False, clear_content=False):
        self.engine = create_engine(engine, echo=echo)
        if drop_all_tables:
            Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine, autocommit=autocommit, autoflush=autoflush)
        self.session = self.Session()
        if clear_content:
            for name, table in Base.metadata.tables.items():
                self.session.execute(table.delete())

    @property
    def pages(self):
        return [page for page in CrawlPage.query(self.session).all()]

    @property
    def seeds(self):
        return self.session.query(CrawlPage).filter_by(is_seed=True).all()

    def add_page(self, url, status=200, n_redirects=0, is_seed=False, commit=True):
        page, created = CrawlPage.get_or_create(self.session, url=url)
        if created:
            page.is_seed = is_seed
        page.status = status
        page.n_redirects = n_redirects
        if commit:
            self.session.commit()
        return page

    def add_link(self, page, url, commit=True, status=200):
        link_page, created = CrawlPage.get_or_create(self.session, url=url)
        if created:
            link_page.status = status
        if link_page not in page.links:
            page.links.append(link_page)
        if commit:
            self.session.commit()
        return link_page

    def get_page(self, url):
        return self.session.query(CrawlPage).filter_by(url=url).first()

    def add_site(self, site, default_status=200, default_n_redirects=0):
        pages = site.pages if isinstance(site, CrawlSiteData) else site
        for i, (info, links) in enumerate(pages):
            if isinstance(info, tuple):
                if len(info) == 2:
                    status, page_url, n_redirects = (info[0], info[1], default_n_redirects)
                else:
                    status, page_url, n_redirects = info
            else:
                status, page_url, n_redirects = (default_status, info, default_n_redirects)
            page = self.add_page(url=page_url, status=status, n_redirects=n_redirects, is_seed=(i == 0))
            for link_url in links:
                self.add_link(page=page, url=link_url, status=default_status)

    def add_site_list(self, graph, default_status=200, default_n_redirects=0):
        sites = graph.sites if isinstance(graph, CrawlSiteListData) else graph
        for site in sites:
            self.add_site(site=site, default_status=default_status, default_n_redirects=default_n_redirects)

    def save(self):
        self.session.commit()

    def render(self, filename, label='', labelloc='t', labeljust='c',
               rankdir="TB", ranksep=0.7,
               fontname='Arial', fontsize=24,
               use_urls=False,
               node_fixedsize='true', nodesep=0.1, node_width=0.85, node_height=0.85, node_fontsize=15,
               include_ids=False):
        import pydot

        # Graph
        graph_args = {
            "rankdir": rankdir,
            "ranksep": ranksep,
            "nodesep": nodesep,
            "fontname": fontname,
            "fontsize": fontsize,
        }
        if label:
            graph_args.update({
                "labelloc": labelloc,
                "labeljust": labeljust,
                "label": label
            })
        graph = pydot.Dot(**graph_args)

        # Node
        node_args = {
            "fontsize": node_fontsize,
        }
        if use_urls:
            node_seed_shape = 'rectangle'
            node_shape = 'oval'
        else:
            node_seed_shape = 'square'
            node_shape = 'circle'
            node_args.update({
                "fixedsize": node_fixedsize,
                "width": node_width,
                "height": node_height,
            })

        graph.set_node_defaults(**node_args)
        for page in self.pages:
            graph.add_node(pydot.Node(name=self._clean_page_name(page, include_id=include_ids),
                                      fontname=fontname,
                                      fontsize=node_fontsize,
                                      shape=node_seed_shape if page.is_seed else node_shape))
            for link in page.links:
                graph.add_edge(pydot.Edge(self._clean_page_name(page, include_id=include_ids),
                                          self._clean_page_name(link, include_id=include_ids)))
        graph.write_png(filename)

    def _clean_page_name(self, page, include_id):
        cleaned_name = page.url
        cleaned_name = cleaned_name.replace('http://', '')
        cleaned_name = cleaned_name.replace('https://', '')
        if include_id:
            cleaned_name = "%d. %s" % (page.id, cleaned_name)
        return cleaned_name