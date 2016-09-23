import os
import unittest
from shutil import rmtree
from tempfile import mkdtemp

from scrapy.spiders import Spider

from frontera.settings import Settings
from frontera.contrib.scrapy.middlewares.seeds.file import FileSeedLoader, NotConfigured


class TestFileSeedLoader(unittest.TestCase):

    def setUp(self):
        self.tmp_path = mkdtemp()

    def tearDown(self):
        rmtree(self.tmp_path)

    def seed_loader_setup(self, seeds_content=None):
        seed_path = os.path.join(self.tmp_path, 'seeds.txt')
        default_content = """
https://www.example.com
https://www.scrapy.org
"""
        seeds_content = seeds_content or default_content
        with open(seed_path, 'wb') as tmpl_file:
            tmpl_file.write(seeds_content.encode('utf-8'))
        assert os.path.isfile(seed_path)  # Failure of test itself
        settings = Settings()
        settings.SEEDS_SOURCE = seed_path
        crawler = type('crawler', (object,), {})
        crawler.settings = settings
        return FileSeedLoader(crawler)

    def test_seeds_not_configured(self):
        crawler = type('crawler', (object,), {})
        crawler.settings = Settings()
        self.assertRaises(NotConfigured, FileSeedLoader, crawler)

    def test_load_seeds(self):
        seed_loader = self.seed_loader_setup()
        seeds = seed_loader.load_seeds()
        self.assertEqual(seeds, ['https://www.example.com', 'https://www.scrapy.org'])

    def test_process_start_requests(self):
        seed_loader = self.seed_loader_setup()
        requests = seed_loader.process_start_requests(None, Spider(name='spider'))
        self.assertEqual([r.url for r in requests], ['https://www.example.com', 'https://www.scrapy.org'])

    def test_process_start_requests_ignore_comments(self):
        seeds_content = """
https://www.example.com
# https://www.dmoz.org
https://www.scrapy.org
# https://www.test.com
"""
        seed_loader = self.seed_loader_setup(seeds_content)
        requests = seed_loader.process_start_requests(None, Spider(name='spider'))
        self.assertEqual([r.url for r in requests], ['https://www.example.com', 'https://www.scrapy.org'])
