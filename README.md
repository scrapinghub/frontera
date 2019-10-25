# Frontera

[![pypi](https://img.shields.io/pypi/v/frontera)](https://pypi.org/project/frontera/)
[![python versions](https://img.shields.io/pypi/pyversions/frontera.svg)](https://pypi.org/project/frontera/)
[![Build Status](https://travis-ci.org/scrapinghub/frontera.svg?branch=master)](https://travis-ci.org/scrapinghub/frontera)
[![codecov](https://codecov.io/gh/scrapinghub/frontera/branch/master/graph/badge.svg)](https://codecov.io/gh/scrapinghub/frontera)

## Overview

Frontera is a web crawling framework consisting of [crawl frontier](http://nlp.stanford.edu/IR-book/html/htmledition/the-url-frontier-1.html), and distribution/scaling primitives, allowing to build a large scale online web crawler. 

Frontera takes care of the logic and policies to follow during the crawl. It stores and prioritises links extracted by 
the crawler to decide which pages to visit next, and capable of doing it in distributed manner.

## Main features

- Online operation: small requests batches, with parsing done right after fetch.
- Pluggable backend architecture: low-level backend access logic is separated from crawling strategy.
- Two run modes: single process and distributed.
- Built-in SqlAlchemy, Redis and HBase backends.
- Built-in Apache Kafka and ZeroMQ message buses.
- Built-in crawling strategies: breadth-first, depth-first, Discovery (with support of robots.txt and sitemaps).
- Battle tested: our biggest deployment is 60 spiders/strategy workers delivering 50-60M of documents daily for 45 days, without downtime,
- Transparent data flow, allowing to integrate custom components easily using Kafka.
- Message bus abstraction, providing a way to implement your own transport (ZeroMQ and Kafka are available out of the box).
- Optional use of Scrapy for fetching and parsing.
- 3-clause BSD license, allowing to use in any commercial product.
- Python 3 support.

## Installation

```bash
$ pip install frontera
```

## Documentation

- [Main documentation at RTD](http://frontera.readthedocs.org/)
- [EuroPython 2015 slides](http://www.slideshare.net/sixtyone/fronteraopen-source-large-scale-web-crawling-framework)
- [BigDataSpain 2015 slides](https://speakerdeck.com/scrapinghub/frontera-open-source-large-scale-web-crawling-framework)

## Community

Join our Google group at https://groups.google.com/a/scrapinghub.com/forum/#!forum/frontera or check GitHub issues and 
pull requests.
