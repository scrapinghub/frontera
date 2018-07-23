# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup


def _process_sitemap(s):
    soup = BeautifulSoup(s, "lxml")
    result = []
    sub_sitemaps = []

    for loc in soup.findAll('loc'):
        if loc.parent.name == 'url':
            result.append(loc.text.strip())
            continue
        if loc.parent.name == 'sitemap':
            sub_sitemaps.append(loc.text.strip())
            continue
    return result, sub_sitemaps


def parse_sitemap(content):
    sitemap, sub_sitemaps = _process_sitemap(content)
    while sitemap:
        yield (sitemap.pop(), False)
    while sub_sitemaps:
        yield (sub_sitemaps.pop(), True)
