import datetime
import random
import copy
from collections import OrderedDict
import string
from pymongo import MongoClient
from crawlfrontier import Backend, Page
import json
from crawlfrontier.contrib.middlewares.domain import Domain
from crawlfrontier.exceptions import NotConfigured


class MongodbBackend(Backend):
    name = 'Mongodb Backend'

    def __init__(self, manager):
        settings = manager.settings
        mongo_ip = settings.get('BACKEND_MONGO_IP', None)
        mongo_port = settings.get('BACKEND_MONGO_PORT', None)
        if mongo_ip is None or mongo_port is None:
            raise NotConfigured
        self.client = MongoClient(mongo_ip, mongo_port)
        self.database_name = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))
        mongo_db = settings.get('BACKEND_MONGO_DB_NAME', self.database_name)
        mongo_collection = settings.get('BACKEND_MONGO_COLLECTION_NAME', 'links')
        self.db = self.client[mongo_db]
        self.collection = self.db[mongo_collection]
        self.manager = manager

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
            # self.collection.insert(json.loads(repr(page)))

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
        self.collection.update({'fingerprint':backend_page.fingerprint}, {"$set": json.loads(repr(backend_page))}, upsert=False)

        # Create links
        for link in links:
            self.manager.logger.backend.debug('ADD_LINK link=%s' % link)
            link_page, link_created = self._get_or_create_page_from_link(link, datetime.datetime.utcnow())
            if link_created:
                link_page.depth = page.depth+1
                self.collection.update({'fingerprint':link_page.fingerprint}, {"$set": json.loads(repr(link_page))}, upsert=False)

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
        self.collection.update({'fingerprint':backend_page.fingerprint}, {"$set": json.loads(repr(backend_page))}, upsert=False)

        # Return updated page
        return backend_page

    def get_next_pages(self, max_next_pages):
        # Log
        self.manager.logger.backend.debug('GET_NEXT_PAGES max_next_pages=%s' % max_next_pages)

        now = datetime.datetime.utcnow()
        # pages = [page for page in self.pages.values() if page.state == self.manager.page_model.State.NOT_CRAWLED]

        mongo_pages = self._get_sorted_pages(max_next_pages)

        pages = []
        for p in mongo_pages:
            page = self._page_from_mongo_dict(p)
            pages.append(page)
            finished = False
        # pages = self._sort_pages(pages)
        if max_next_pages:
            pages = pages[0:max_next_pages]
        for page in pages:
            page.state = self.manager.page_model.State.QUEUED
            page.n_queued += 1
            page.last_update = now
            self.collection.update({'fingerprint':page.fingerprint}, {"$set": json.loads(repr(page))}, upsert=False)
        remaining = self.collection.find_one({'state': {'$ne': self.manager.page_model.State.CRAWLED}})
        if remaining is None:
            self.manager._finished = True
        return pages


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

    def _page_from_mongo_dict(self, mongo_dict):
        page = self.manager.page_model(mongo_dict['url'])
        page.status = mongo_dict['status']
        domain = Domain(mongo_dict['domain']['netloc'], mongo_dict['domain']['name'], mongo_dict['domain']['scheme'], mongo_dict['domain']['sld'], mongo_dict['domain']['tld'], mongo_dict['domain']['subdomain'])
        page.domain = domain
        page.n_crawls = mongo_dict['n_crawls']
        page.created_at = mongo_dict['created_at']
        page.state = mongo_dict['state']
        page.last_update = mongo_dict['last_update']
        page.depth = mongo_dict['depth']
        page.meta = mongo_dict['meta']
        page.n_errors = mongo_dict['n_errors']
        page.fingerprint = mongo_dict['fingerprint']
        page.n_queued = mongo_dict['n_queued']
        page.n_adds = mongo_dict['n_adds']
        return page


    def _get_or_create_page_from_link(self, link, now):
        fingerprint = link.fingerprint
        existing_page = self.collection.find_one({'fingerprint': fingerprint})
        if existing_page is None:
            new_page = self.manager.page_model.from_link(link)
            # self.pages[fingerprint] = new_page
            new_page.created_at = now
            self.collection.insert(json.loads(repr(new_page)))
            self.manager.logger.backend.debug('Creating page %s from link %s' % (new_page, link))
            return new_page, True
        else:
            page = self._page_from_mongo_dict(existing_page)
            # page = self.pages[fingerprint]
            self.manager.logger.backend.debug('Page %s exists' % page)
            return page, False

    def _get_or_create_page_from_page(self, page, now):
        fingerprint = page.fingerprint
        existing_page = self.collection.find_one({'fingerprint': fingerprint})
        if existing_page is None:
            new_page = copy.deepcopy(page)
            # self.pages[fingerprint] = new_page
            new_page.created_at = now
            self.collection.insert(json.loads(repr(new_page)))
            self.manager.logger.backend.debug('Creating page %s from page %s' % (new_page, page))
            return new_page, True
        else:
            page = self._page_from_mongo_dict(existing_page)
            self.manager.logger.backend.debug('Page %s exists' % page)
            # page = self.pages[fingerprint]
            return page, False

    def _get_sorted_pages(self, max_pages):
        raise NotImplementedError


    def frontier_stop(self):
        if self.manager.settings.get('BACKEND_MONGO_PERSIST_INFO', True) is False:
            self.client.drop_database(self.database_name)
        self.client.close()


class MongodbFIFOBackend(MongodbBackend):
    name = 'FIFO Mongodb Backend'

    def _get_sorted_pages(self, max_pages):
        mongoPages = self.collection.find({'state': self.manager.page_model.State.NOT_CRAWLED}).limit(max_pages)
        return mongoPages



class MongodbLIFOBackend(MongodbBackend):
    name = 'LIFO Mongodb Backend'

    def _get_sorted_pages(self, max_pages):
        mongoPages = self.collection.find({'state': self.manager.page_model.State.NOT_CRAWLED}).sort('_id', -1).limit(max_pages)
        return mongoPages



class MongodbDFSBackend(MongodbBackend):
    name = 'DFS Mongodb Backend'

    def _get_sorted_pages(self, max_pages):
        mongoPages = self.collection.find({'state': self.manager.page_model.State.NOT_CRAWLED}).sort('depth', -1).limit(max_pages)
        return mongoPages




class MongodbBFSBackend(MongodbBackend):
    name = 'BFS Mongodb Backend'

    def _get_sorted_pages(self, max_pages):
        mongoPages = self.collection.find({'state': self.manager.page_model.State.NOT_CRAWLED}).sort('depth', 1).limit(max_pages)
        return mongoPages




BASE = MongodbBackend
FIFO = MongodbFIFOBackend
LIFO = MongodbLIFOBackend
DFS = MongodbDFSBackend
BFS = MongodbBFSBackend
