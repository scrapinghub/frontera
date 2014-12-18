import datetime
import random
import copy
from collections import OrderedDict
import string
from bson import json_util
from pymongo import MongoClient
from crawlfrontier import Backend, Request
import json
from crawlfrontier.exceptions import NotConfigured


class MongodbBackend(Backend):
    name = 'Mongodb Backend'

    class State:
        NOT_CRAWLED = 'NOT CRAWLED'
        QUEUED = 'QUEUED'
        CRAWLED = 'CRAWLED'
        ERROR = 'ERROR'

    def __init__(self, manager):
        settings = manager.settings
        mongo_ip = settings.get('BACKEND_MONGO_IP', None)
        mongo_port = settings.get('BACKEND_MONGO_PORT', None)
        if mongo_ip is None or mongo_port is None:
            raise NotConfigured
        self.client = MongoClient(mongo_ip, mongo_port)
        self.database_name = ''.join(
            random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(10))
        mongo_db = settings.get('BACKEND_MONGO_DB_NAME', self.database_name)
        mongo_collection = settings.get('BACKEND_MONGO_COLLECTION_NAME', 'links')
        self.db = self.client[mongo_db]
        self.collection = self.db[mongo_collection]
        self.manager = manager

    @classmethod
    def from_manager(cls, manager):
        return cls(manager)

    def add_seeds(self, seeds):
        # Log
        self.manager.logger.backend.debug('ADD_SEEDS n_links=%s' % len(seeds))

        for seed in seeds:
            # Get or create page from link
            request, _ = self._get_or_create_request(seed)

    def page_crawled(self, response, links):
        # Log
        self.manager.logger.backend.debug('PAGE_CRAWLED page=%s status=%s links=%s' %
                                          (response, response.status_code, len(links)))

        # process page crawled
        backend_page = self._page_crawled(response)

        # Update crawled fields
        backend_page.state = self.State.CRAWLED
        self.collection.update({'_meta.fingerprint': backend_page._meta['fingerprint']}, {
            "$set": self._request_to_mongo(backend_page)}, upsert=False)

        # Create links
        for link in links:
            self.manager.logger.backend.debug('ADD_LINK link=%s' % link)
            link_page, link_created = self._get_or_create_request(link)
            if link_created:
                link_page._meta['depth'] = response.meta['depth'] + 1
                self.collection.update({'_meta.fingerprint': link_page._meta['fingerprint']}, {
                    "$set": self._request_to_mongo(link_page)}, upsert=False)

    def request_error(self, page, error):
        # Log
        self.manager.logger.backend.debug('PAGE_CRAWLED_ERROR page=%s error=%s' % (page, error))

        # process page crawled
        backend_page = self._page_crawled(page)

        # Update error fields
        backend_page.state = self.State.ERROR
        self.collection.update({'_meta.fingerprintfingerprint': backend_page._meta['fingerprint']},
                               {"$set": self._request_to_mongo(link_page)}, upsert=False)

        # Return updated page
        return backend_page

    def get_next_requests(self, max_next_pages):
        # Log
        self.manager.logger.backend.debug('GET_NEXT_PAGES max_next_pages=%s' % max_next_pages)
        now = datetime.datetime.utcnow()
        mongo_pages = self._get_sorted_pages(max_next_pages)
        requests = []
        for p in mongo_pages:
            req = self._request_from_mongo_dict(p)
            requests.append(req)

        if max_next_pages:
            requests = requests[0:max_next_pages]
        for req in requests:
            req.state = self.State.QUEUED
            req.last_update = now
            self.collection.update({'_meta.fingerprint': req._meta['fingerprint']}, {
                "$set": self._request_to_mongo(req)}, upsert=False)
        remaining = self.collection.find_one({'state': {'$ne': self.State.CRAWLED}})
        if remaining is None:
            self.manager._finished = True
        return requests

    def _page_crawled(self, response):
        # Get timestamp
        now = datetime.datetime.utcnow()

        # Get or create page from incoming page
        backend_page, created = self._get_or_create_request(response)

        # Update creation fields
        if created:
            backend_page.created_at = now

        # Update fields
        backend_page.last_update = now
        backend_page.status = response.status_code
        return backend_page

    def _request_to_mongo(self, request):
        return json.loads(json.dumps(vars(request), sort_keys=True, indent=4, default=json_util.default),
                          object_hook=json_util.object_hook)

    def _request_from_mongo_dict(self, mongo_dict):
        url = mongo_dict['_url']
        method = mongo_dict['_method']
        headers = mongo_dict['_headers']
        cookies = mongo_dict['_cookies']
        meta = mongo_dict['_meta']
        request = Request(url, method, headers, cookies, meta)
        return request

    def _get_or_create_request(self, request):
        fingerprint = request.meta['fingerprint']
        existing_request = self.collection.find_one({'_meta.fingerprint': fingerprint})
        if existing_request is None:
            new_request = request.copy()
            new_request.meta['created_at'] = datetime.datetime.utcnow()
            new_request.meta['depth'] = 0
            new_request.state = self.State.NOT_CRAWLED
            self.collection.insert(
                json.loads(json.dumps(vars(new_request), sort_keys=True, indent=4, default=json_util.default),
                           object_hook=json_util.object_hook))
            self.manager.logger.backend.debug('Creating request %s' % new_request)
            return new_request, True
        else:
            request = self._request_from_mongo_dict(existing_request)
            self.manager.logger.backend.debug('Request exists %s' % request)
            return request, False

    def _get_sorted_pages(self, max_pages):
        raise NotImplementedError

    def frontier_start(self):
        pass

    def frontier_stop(self):
        if self.manager.settings.get('BACKEND_MONGO_PERSIST_INFO', True) is False:
            self.client.drop_database(self.database_name)
        self.client.close()


class MongodbFIFOBackend(MongodbBackend):
    name = 'FIFO Mongodb Backend'

    def _get_sorted_pages(self, max_pages):
        mongo_requests = self.collection.find({'state': self.State.NOT_CRAWLED}).limit(max_pages)
        return mongo_requests


class MongodbLIFOBackend(MongodbBackend):
    name = 'LIFO Mongodb Backend'

    def _get_sorted_pages(self, max_pages):
        mongo_requests = self.collection.find({'state': self.State.NOT_CRAWLED}).sort('_id', -1).limit(max_pages)
        return mongo_requests


class MongodbDFSBackend(MongodbBackend):
    name = 'DFS Mongodb Backend'

    def _get_sorted_pages(self, max_pages):
        mongo_requests = self.collection.find({'state': self.State.NOT_CRAWLED}).sort('depth', -1).limit(max_pages)
        return mongo_requests


class MongodbBFSBackend(MongodbBackend):
    name = 'BFS Mongodb Backend'

    def _get_sorted_pages(self, max_pages):
        mongo_requests = self.collection.find({'state': self.State.NOT_CRAWLED}).sort('depth', 1).limit(max_pages)
        return mongo_requests


BASE = MongodbBackend
FIFO = MongodbFIFOBackend
LIFO = MongodbLIFOBackend
DFS = MongodbDFSBackend
BFS = MongodbBFSBackend
