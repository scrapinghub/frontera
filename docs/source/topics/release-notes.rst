=============
Release Notes
=============

0.2.0 (released 2015-01-12)
===========================

- Added documentation (Scrapy Seed Loaders+Tests+Examples) (:commit:`8e5f60d`)
- Refactored backend tests (:commit:`00910bf`, :commit:`5702bef`, :commit:`9567566`)
- Added requests library example (:commit:`8796011`)
- Added requests library manager and object converters (:commit:`d6590b6`)
- Added FrontierManagerWrapper (:commit:`4f04a48`)
- Added frontier object converters (:commit:`7da51a4`)
- Fixed script examples for new changes (:commit:`101ea27`)
- Optional Color logging (only if available) (:commit:`c0ba0ba`)
- Changed Scrapy frontier and recorder integration to scheduler+middlewares (:commit:`cbe5f4f` / :commit:`2fcdc06` / :commit:`f7bf02b` / :commit:`0d15dc1`)
- Changed default frontier backend (:commit:`03cd307`)
- Added comment support to seeds (:commit:`7d48973`)
- Added doc requirements for RTD build (:commit:`27daea4`)
- Removed optional dependencies for setup.py and requirements (:commit:`c6099f3` / :commit:`79a4e4d` / :commit:`e6910e3`)
- Changed tests to pytest (:commit:`848d2bf` / :commit:`edc9c01` / :commit:`c318d14`)
- Updated docstrings and documentation (:commit:`fdccd92` / :commit:`9dec38c` / :commit:`71d626f` / :commit:`0977bbf`)
- Changed frontier componets (Backend and Middleware) to abc (:commit:`1e74467`)
- Modified Scrapy frontier example to use seed loaders (:commit:`0ad905d`)
- Refactored Scrapy Seed loaders (:commit:`a0eac84`)
- Added new fields to ``Request`` and ``Response`` frontier objects (:commit:`bb64afb`)
- Added ``ScrapyFrontierManager`` (Scrapy wrapper for Frontier Manager) (:commit:`8e50dc0`)
- Changed frontier core objects (``Page``/``Link`` to ``Request``/``Response``) (:commit:`74b54c8`)


0.1
===

First release of Crawl Frontier.