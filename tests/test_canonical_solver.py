# -*- coding: utf-8 -*-
from frontera.contrib.canonicalsolvers import Basic, CorporateWebsiteFriendly
from frontera.core.models import Request, Response
from frontera.utils.fingerprint import sha1


def single_node_chain(url1, url2):
    r = Request(url=url1)
    re = Response(url=url2, request=r)
    re.meta['fingerprint'] = sha1(url2)
    re.meta['redirect_urls'] = [url1]
    re.meta['redirect_fingerprints'] = [sha1(url1)]
    return re


def test_basic():
    cs = Basic()
    re = single_node_chain("http://www.scrapinghub.com/", "http://scrapinghub.com/")
    cs.page_crawled(re, [])
    assert re.url == "http://www.scrapinghub.com/"


def test_corporate_website_friendly():
    cs = CorporateWebsiteFriendly()

    # check the Basic CS behavior
    re = single_node_chain("http://www.yandex.ru/company/", "http://google.com/404")
    cs.page_crawled(re, [])
    assert re.url == "http://www.yandex.ru/company/"

    # redirect to home page is allowed
    re = single_node_chain("http://www.yandex.ru", "http://google.com")
    cs.page_crawled(re, [])
    assert re.url == "http://google.com"

    # redirect within to internal page is also allowed
    re = single_node_chain("http://www.yandex.ru", "http://www.yandex.ru/search")
    cs.page_crawled(re, [])
    assert re.url == "http://www.yandex.ru/search"

