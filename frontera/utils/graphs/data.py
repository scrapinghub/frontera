from __future__ import absolute_import
from six.moves import range


def create_test_site(prefix, max_depth, n_links_per_page, self_link=False, site=None, depth=0):
    if not site:
        site = []
        prefix += str(1)
    depth += 1
    if depth < max_depth:
        page = prefix
        links = [page + str(l) for l in range(1, n_links_per_page+1)]
        site.append((page, links))
        for link in links:
            create_test_site(prefix=link,
                             max_depth=max_depth,
                             n_links_per_page=n_links_per_page,
                             self_link=self_link,
                             site=site,
                             depth=depth)
        if self_link:
            links.append(page)
    return site


class CrawlSiteData(object):
    def __init__(self, pages, name='', description=''):
        self.name = name
        self.description = description
        self.pages = pages

    def __repr__(self):
        return '<CrawlSite:%s[%s]>' % (self.name, len(self.pages))

    @property
    def nodes(self):
        n = set()
        for page, links in self.pages:
            n.add(page)
            for link in links:
                n.add(link)
        return n

    def __len__(self):
        return len(self.nodes)


class CrawlSiteListData(object):
    def __init__(self, sites, name='', description='', use_urls=False):
        self.name = name
        self.description = description
        self.sites = sites
        self.use_urls = use_urls

    def __repr__(self):
        return '<CrawlSiteList:%s[%s]>' % (self.name, len(self.sites))

    def __len__(self):
        return sum([len(site) for site in self.sites])


#-----------------------------------------------------
# Sites
#-----------------------------------------------------
SITE_A = CrawlSiteData(
    name='A',
    description='',
    pages=create_test_site('http://aaa.com/', 4, 2))

SITE_B = CrawlSiteData(
    name='B',
    description='',
    pages=create_test_site('http://bbb.com/', 4, 2))

SITE_C = CrawlSiteData(
    name='C',
    description='',
    pages=create_test_site('http://ccc.com/', 5, 2, self_link=True))


#-----------------------------------------------------
# Graphs
#-----------------------------------------------------
SITE_LIST_01 = CrawlSiteListData(
    name='GRAPH 01',
    description='',
    sites=[
        SITE_A,
    ])

SITE_LIST_02 = CrawlSiteListData(
    name='GRAPH 02',
    description='',
    sites=[
        SITE_A,
        SITE_B,
    ])

SITE_LIST_03 = CrawlSiteListData(
    name='GRAPH 03',
    description='',
    sites=[
        SITE_C,
    ])

SITE_LIST_04 = CrawlSiteListData(
    name='GRAPH 04',
    description='',
    sites=[
        [
            ('A', ['B']),
            ('B', ['A']),
        ],
    ])

SITE_LIST_05 = CrawlSiteListData(
    name='GRAPH 05',
    description='',
    sites=[
        [
            ('A', ['B', 'C']),
            ('B', ['A', 'C']),
            ('C', ['A', 'B']),
        ],
    ])

SITE_LIST_06 = CrawlSiteListData(
    name='GRAPH 06',
    description='',
    sites=[
        [
            ('A', ['B', 'C']),
            ('B', []),
            ('C', ['B']),
        ]
    ])

SITE_LIST_07 = CrawlSiteListData(
    name='GRAPH 07',
    description='',
    sites=[
        [
            ('A', ['A']),
        ]
    ])

SITE_LIST_08 = CrawlSiteListData(
    name='GRAPH 08',
    description='',
    use_urls=True,
    sites=[
        [
            ('https://www.a.com', [
                'http://www.a.com/2',
                'http://www.a.net',
            ]),
        ],
        [
            ('https://www.a.net', []),
        ],
        [
            ('http://b.com', [
                'http://b.com/2',
                'http://www.a.net',
                'http://test.cloud.c.com',
                'http://b.com',
            ]),
            ('http://b.com/entries?page=2', [
                'http://b.com/entries?page=2',
                'http://b.com',
            ]),
        ],
        [
            ('http://test.cloud.c.com', [
                'http://cloud.c.com',
                'http://test.cloud.c.com/2',
            ]),
            ('http://test.cloud.c.com/2', [
                'http://b.com/entries?page=2',
                'http://test.cloud.c.com',
            ]),
        ],
    ])

SITE_LIST_09 = CrawlSiteListData(
    name='GRAPH 09',
    description='',
    use_urls=True,
    sites=[
        [
            ('https://www.a.com', [
                'http://www.a.com/2',
                'http://www.a.com/2/1',
                'http://www.a.com/3',
                'http://www.a.com/2/1/3',
                'http://www.a.com/2/4/1',
                'http://www.a.com/2/4/2',
                'http://www.a.net',
            ]),
        ],
        [
            ('http://b.com', [
                'http://b.com/2',
                'http://www.a.net',
                'http://test.cloud.c.com',
                'http://b.com',
            ]),
            ('http://b.com/entries?page=2', [
                'http://b.com/entries?page=2',
                'http://b.com',
            ]),
        ],
        [
            ('http://test.cloud.c.com', [
                'http://cloud.c.com',
                'http://test.cloud.c.com/2',
            ]),
            ('http://test.cloud.c.com/2', [
                'http://b.com/entries?page=2',
                'http://test.cloud.c.com',
            ]),
        ],
    ])

GRAPHS = [obj for obj in locals().values() if isinstance(obj, CrawlSiteListData)]
#GRAPHS = [SITE_LIST_08]
