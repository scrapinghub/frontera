from six.moves.urllib.parse import urlparse
from boto import connect_s3
from scrapy.exceptions import NotConfigured

from frontera.contrib.scrapy.middlewares.seeds.file import FileSeedLoader


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
