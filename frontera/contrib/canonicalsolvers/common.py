# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .basic import BasicCanonicalSolver
from frontera.utils.url import parse_url


class CorporateWebsiteFriendly(BasicCanonicalSolver):

    def _set_canonical(self, obj):
        if b'redirect_urls' in obj.meta:
            # if home page is requested then leave the target page as canonical
            urls = obj.meta[b'redirect_urls']
            scheme, netloc, path, params, query, fragment = parse_url(urls[0])
            if not path or path in ['/', 'index.html', 'index.htm', 'default.htm']:
                return

            # check if redirect is within the same hostname
            target = parse_url(obj.url)
            src_hostname, _, _ = netloc.partition(':')
            trg_hostname, _, _ = target.netloc.partition(':')
            if src_hostname == trg_hostname:
                return

            # otherwise default behavior
            super(CorporateWebsiteFriendly, self)._set_canonical(obj)
