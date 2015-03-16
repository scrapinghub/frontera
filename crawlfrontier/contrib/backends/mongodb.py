from datetime import datetime
from pymongo import MongoClient, DESCENDING

from crawlfrontier import Backend, Request, Response
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
        mongo_hostname = settings.get('BACKEND_MONGO_HOSTNAME')
        mongo_port = settings.get('BACKEND_MONGO_PORT')
        mongo_db = settings.get('BACKEND_MONGO_DB_NAME')
        mongo_collection = settings.get('BACKEND_MONGO_COLLECTION_NAME')
        if mongo_hostname is None or mongo_port is None or mongo_db is None or mongo_collection is None:
            raise NotConfigured

        self.client = MongoClient(mongo_hostname, mongo_port)
        self.db = self.client[mongo_db]
        self.collection = self.db[mongo_collection]
        self.collection.ensure_index("meta.fingerprint", unique=True, drop_dups=True)
        self.collection.ensure_index("score")
        self.collection.ensure_index("meta.created_at")
        self.collection.ensure_index("meta.depth")
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
        self.collection.update(self._get_mongo_spec(backend_page), {
            "$set": self._to_mongo_dict(backend_page)}, upsert=False)

        # Create links
        for link in links:
            self.manager.logger.backend.debug('ADD_LINK link=%s' % link)
            link_page, link_created = self._get_or_create_request(link)
            if link_created:
                link_page._meta['depth'] = response.meta['depth'] + 1
                self.collection.update(self._get_mongo_spec(link_page), {
                    "$set": self._to_mongo_dict(link_page)}, upsert=False)

    def _page_crawled(self, response):
        # Get timestamp
        now = datetime.utcnow()

        # Get or create page from incoming page
        backend_page, created = self._get_or_create_request(response)

        # Update creation fields
        if created:
            backend_page.created_at = now

        # Update fields
        backend_page.last_update = now
        backend_page.status = response.status_code
        return backend_page

    def request_error(self, request, error):
        self.manager.logger.backend.debug('PAGE_CRAWLED_ERROR page=%s error=%s' % (request, error))
        now = datetime.utcnow()

        backend_page, created = self._get_or_create_request(request)

        if created:
            backend_page.created_at = now
        backend_page.last_update = now

        backend_page.state = self.State.ERROR
        self.collection.update(self._get_mongo_spec(backend_page),
                               {"$set": self._to_mongo_dict(backend_page)}, upsert=False)
        return backend_page

    def get_next_requests(self, max_next_pages, downloader_info=None):
        # Log
        self.manager.logger.backend.debug('GET_NEXT_PAGES max_next_pages=%s' % max_next_pages)
        now = datetime.utcnow()
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
            self.collection.update(self._get_mongo_spec(req), {
                "$set": self._to_mongo_dict(req)}, upsert=False)
        return requests

    def _get_mongo_spec(self, obj):
        return {'meta.fingerprint': obj.meta['fingerprint']}

    def _request_from_mongo_dict(self, o):
        request = Request(o['url'], o['method'], o['headers'], o['cookies'], o['meta'])
        request.state = o['state']
        return request

    def _to_mongo_dict(self, obj):
        def _request_to_dict(req):
            return {
                'url': req.url,
                'method': req.method,
                'headers': req.headers,
                'cookies': req.cookies,
                'meta': req.meta,
                'state': req.state
            }

        if isinstance(obj, Request):
            return _request_to_dict(obj)

        if isinstance(obj, Response):
            return {
                'url': obj.url,
                'status_code': obj.status_code,
                'headers': obj.headers,
                'body': obj.body,
                'meta': obj.request.meta,
                'method': obj.request.method,
                'cookies': obj.request.cookies,
                'state': obj.state
            }

        raise TypeError("Type of object %s isn't known." % obj)

    def _get_or_create_request(self, obj):
        existing_request = self.collection.find_one(self._get_mongo_spec(obj))
        if existing_request is None:
            new_request = obj.copy()
            new_request.meta['created_at'] = datetime.utcnow()
            new_request.meta['depth'] = 0
            new_request.state = self.State.NOT_CRAWLED
            self.collection.insert(self._to_mongo_dict(new_request))
            self.manager.logger.backend.debug('Creating request %s' % new_request)
            return new_request, True
        else:
            obj = self._request_from_mongo_dict(existing_request)
            self.manager.logger.backend.debug('Request exists %s' % obj)
            return obj, False

    def _get_sorted_pages(self, max_pages):
        raise NotImplementedError

    def frontier_start(self):
        pass

    def frontier_stop(self):
        self.client.close()


class MongodbScoreBackend(MongodbBackend):
    name = 'Score Mongodb Backend'

    def _get_sorted_pages(self, max_pages):
        return self.collection.find({'state': self.State.NOT_CRAWLED}).sort('meta.score', DESCENDING).limit(max_pages)