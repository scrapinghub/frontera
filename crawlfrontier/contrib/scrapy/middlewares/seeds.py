
import codecs
from boto import connect_s3
from scrapy.http import Request
from scrapy.exceptions import NotConfigured
from six.moves.urllib.parse import urlparse

class SeedLoader(object):
    def __init__(self, crawler):
        self.crawler = crawler
        self.configure(crawler.settings)

    def configure(self, settings):
        raise NotImplementedError

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_start_requests(self, start_requests, spider):
        for request in self.load_seeds():
            yield request

    def load_seeds(self):
        raise NotImplementedError


class FileSeedLoader(SeedLoader):

    def configure(self, settings):
        self.seeds_source = settings.get('SEEDS_SOURCE')
        if not self.seeds_source:
            raise NotConfigured

    def load_seeds(self):
        # TODO check if it's an existing file or a folder
        return self.load_seeds_from_file(self.seeds_source)

    def load_seeds_from_file(self, file_path):
        with codecs.open(file_path, 'rU') as f:
            return self.load_seeds_from_data((f))

    def load_seeds_from_data(self, data):
        seeds = []
        for seed in data:
            clean_seed = self.clean_seed(seed)
            if clean_seed:
                seeds.append(Request(clean_seed))
        return seeds

    def clean_seed(self, url):
        return url.strip(' \t\n\r')


class S3SeedLoader(FileSeedLoader):

    def configure(self, settings):
        source = settings.get('SEEDS_SOURCE')
        u = urlparse(source)
        if not u.hostname or not u.scheme == 's3':
            raise NotConfigured
        self.bucket_name = u.hostname
        self.bucket_keys_prefix = u.path.lstrip('/')
        self.s3_aws_access_key = settings.get('SEEDS_AWS_ACCESS_KEY')
        self.s3_aws_secret_key = settings.get('SEEDS_AWS_SECRET_ACCESS_KEY')
        if not self.s3_aws_access_key or not self.s3_aws_secret_key:
            raise NotConfigured

    def load_seeds(self):
        conn = connect_s3(self.s3_aws_access_key,
                          self.s3_aws_secret_key)
        bucket = conn.get_bucket(self.bucket_name)
        seeds = []
        for key in bucket.list(self.bucket_keys_prefix):
            if key.name.endswith(".txt"):
                data = key.get_contents_as_string(encoding='utf-8').split()
                file_seeds = self.load_seeds_from_data(data)
                seeds.extend(file_seeds)
        return seeds
