# -*- coding: utf-8 -*-
from frontera.contrib.canonicalsolvers import Basic
from frontera.core.models import Request, Response


def test_basic():
    cs = Basic()
    r = Request(url="http://www.scrapinghub.com/")

    re = Response(url="http://scrapinghub.com/", request=r)
    re.meta['fingerprint'] = "6d8afb0c246caa28a2c1bdaaac19c70c24a2d22e"
    re.meta['redirect_urls'] = ['http://www.scrapinghub.com/']
    re.meta['redirect_fingerprints'] = ["6cd0a1e069d5a1666a6ec290a4b33f5f325c2e66"]
    cs.page_crawled(re, [])
    assert re.url == "http://www.scrapinghub.com/"