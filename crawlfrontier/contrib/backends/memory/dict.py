import datetime
import random
import copy
from collections import OrderedDict

from crawlfrontier import Backend


class MemoryBackend(Backend):
    name = 'Memory Backend'

    def __init__(self, manager):
        self.manager = manager
        self.pages = OrderedDict()

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def add_seeds(self, links):
        # Log
        self.manager.logger.backend.debug('ADD_SEEDS n_links=%s' % len(links))

        pages = []
        for link in links:
            # Get timestamp
            now = datetime.datetime.utcnow()

            # Get or create page from link
            page, created = self._get_or_create_page_from_link(link, now)

            # Update add fields
            page.n_adds += 1
            page.last_update = now
            pages.append(page)

        # Return updated pages
        return pages

    def page_crawled(self, page, links):
        # Log
        self.manager.logger.backend.debug('PAGE_CRAWLED page=%s status=%s links=%s' %
                                          (page, page.status, len(links)))

        # process page crawled
        backend_page = self._page_crawled(page)

        # Update crawled fields
        backend_page.n_crawls += 1
        backend_page.state = self.manager.page_model.State.CRAWLED

        # Create links
        for link in links:
            self.manager.logger.backend.debug('ADD_LINK link=%s' % link)
            link_page, link_created = self._get_or_create_page_from_link(link, datetime.datetime.utcnow())
            if link_created:
                link_page.depth = page.depth+1

        # Return updated page
        return backend_page

    def page_crawled_error(self, page, error):
        # Log
        self.manager.logger.backend.debug('PAGE_CRAWLED_ERROR page=%s error=%s' % (page, error))

        # process page crawled
        backend_page = self._page_crawled(page)

        # Update error fields
        backend_page.n_errors += 1
        backend_page.state = self.manager.page_model.State.ERROR

        # Return updated page
        return backend_page

    def get_next_pages(self, max_next_pages):
        # Log
        self.manager.logger.backend.debug('GET_NEXT_PAGES max_next_pages=%s' % max_next_pages)

        now = datetime.datetime.utcnow()
        pages = [page for page in self.pages.values() if page.state == self.manager.page_model.State.NOT_CRAWLED]
        pages = self._sort_pages(pages)
        if max_next_pages:
            pages = pages[0:max_next_pages]
        for page in pages:
            page.state = self.manager.page_model.State.QUEUED
            page.n_queued += 1
            page.last_update = now
        return pages

    def get_page(self, link):
        return self.pages.get(link.fingerprint, None)

    def _page_crawled(self, page):
        # Get timestamp
        now = datetime.datetime.utcnow()

        # Get or create page from incoming page
        backend_page, created = self._get_or_create_page_from_page(page, now)

        # Update creation fields
        if created:
            backend_page.created_at = now

        # Update fields
        backend_page.last_update = now
        backend_page.status = page.status

        return backend_page

    def _get_or_create_page_from_link(self, link, now):
        fingerprint = link.fingerprint
        if not fingerprint in self.pages:
            new_page = self.manager.page_model.from_link(link)
            self.pages[fingerprint] = new_page
            new_page.created_at = now
            self.manager.logger.backend.debug('Creating page %s from link %s' % (new_page, link))
            return new_page, True
        else:
            page = self.pages[fingerprint]
            self.manager.logger.backend.debug('Page %s exists' % page)
            return page, False

    def _get_or_create_page_from_page(self, page, now):
        fingerprint = page.fingerprint
        if not fingerprint in self.pages:
            new_page = copy.deepcopy(page)
            self.pages[fingerprint] = new_page
            new_page.created_at = now
            self.manager.logger.backend.debug('Creating page %s from page %s' % (new_page, page))
            return new_page, True
        else:
            self.manager.logger.backend.debug('Page %s exists' % page)
            return self.pages[fingerprint], False

    def _sort_pages(self, pages):
        raise NotImplementedError


class MemoryFIFOBackend(MemoryBackend):
    name = 'FIFO Memory Backend'

    def _sort_pages(self, pages):
        return sorted(pages, key=lambda p: p.created_at)


class MemoryLIFOBackend(MemoryBackend):
    name = 'LIFO Memory Backend'

    def _sort_pages(self, pages):
        return sorted(pages, key=lambda p: p.created_at, reverse=True)


class MemoryDFSBackend(MemoryBackend):
    name = 'DFS Memory Backend'

    def _sort_pages(self, pages):
        return sorted(pages, key=lambda p: p.depth, reverse=True)


class MemoryBFSBackend(MemoryBackend):
    name = 'BFS Memory Backend'

    def _sort_pages(self, pages):
        return sorted(pages, key=lambda p: p.depth)


class MemoryRandomBackend(MemoryBackend):
    name = 'RANDOM Memory Backend'

    def _sort_pages(self, pages):
        random.shuffle(pages)
        return pages

BASE = MemoryBackend
FIFO = MemoryFIFOBackend
LIFO = MemoryLIFOBackend
DFS = MemoryDFSBackend
BFS = MemoryBFSBackend
RANDOM = MemoryRandomBackend