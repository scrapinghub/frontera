# -*- coding: utf-8 -*-
from frontera.utils.fingerprint import hostname_local_fingerprint


def test_local_hostname_frongerprint():
    url1 = "https://news.yandex.ru/yandsearch?cl4url=top.rbc.ru/politics/14/07/2015/55a50b509a79473f583e104c&lang=ru&lr=54#fragment"
    assert hostname_local_fingerprint(url1) == '1be68ff556fd0bbe5802d1a100850da29f7f15b1'

    url2 = "TestString"
    assert hostname_local_fingerprint(url2) == 'd598b03bee8866ae03b54cb6912efdfef107fd6d'

