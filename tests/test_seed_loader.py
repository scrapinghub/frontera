import pytest

pytest.importorskip("boto")
pytest.importorskip("scrapy")

import unittest
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp
from unittest import mock

from scrapy.spiders import Spider

from frontera.contrib.scrapy.middlewares.seeds.file import FileSeedLoader, NotConfigured
from frontera.contrib.scrapy.middlewares.seeds.s3 import S3SeedLoader
from frontera.settings import Settings
from tests.mocks.boto import MockConnection


class TestFileSeedLoader(unittest.TestCase):
    def setUp(self):
        self.tmp_path = Path(mkdtemp())

    def tearDown(self):
        rmtree(self.tmp_path)

    def seed_loader_setup(self, seeds_content=None):
        seed_path = self.tmp_path / "seeds.txt"
        default_content = """
https://www.example.com
https://www.scrapy.org
"""
        seeds_content = seeds_content or default_content
        with seed_path.open("wb") as tmpl_file:
            tmpl_file.write(seeds_content.encode("utf-8"))
        assert seed_path.is_file()  # Failure of test itself
        settings = Settings()
        settings.SEEDS_SOURCE = str(seed_path)
        crawler = type("crawler", (object,), {})
        crawler.settings = settings
        return FileSeedLoader(crawler)

    def test_seeds_not_configured(self):
        crawler = type("crawler", (object,), {})
        crawler.settings = Settings()
        self.assertRaises(NotConfigured, FileSeedLoader, crawler)

    def test_load_seeds(self):
        seed_loader = self.seed_loader_setup()
        seeds = seed_loader.load_seeds()
        self.assertEqual(seeds, ["https://www.example.com", "https://www.scrapy.org"])

    def test_process_start_requests(self):
        seed_loader = self.seed_loader_setup()
        requests = seed_loader.process_start_requests(None, Spider(name="spider"))
        self.assertEqual(
            [r.url for r in requests],
            ["https://www.example.com", "https://www.scrapy.org"],
        )

    def test_process_start_requests_ignore_comments(self):
        seeds_content = """
https://www.example.com
# https://www.dmoz.org
https://www.scrapy.org
# https://www.test.com
"""
        seed_loader = self.seed_loader_setup(seeds_content)
        requests = seed_loader.process_start_requests(None, Spider(name="spider"))
        self.assertEqual(
            [r.url for r in requests],
            ["https://www.example.com", "https://www.scrapy.org"],
        )


class TestS3SeedLoader(unittest.TestCase):
    def setUp(self):
        self.tmp_path = Path(mkdtemp())
        settings = Settings()
        settings.SEEDS_SOURCE = "s3://some-bucket/seeds-folder"
        settings.SEEDS_AWS_ACCESS_KEY = "access_key"
        settings.SEEDS_AWS_SECRET_ACCESS_KEY = "secret_key"
        crawler = type("crawler", (object,), {})
        crawler.settings = settings
        self.seed_path_1 = self.tmp_path / "seeds1.txt"
        self.seed_path_2 = self.tmp_path / "seeds2.txt"
        s1_content = """
https://www.example.com
https://www.scrapy.org
"""
        s2_content = """
https://www.dmoz.org
https://www.test.com
"""

        with self.seed_path_1.open("wb") as tmpl_file:
            tmpl_file.write(s1_content.encode("utf-8"))
        with self.seed_path_2.open("wb") as tmpl_file:
            tmpl_file.write(s2_content.encode("utf-8"))
        self.seed_loader = S3SeedLoader(crawler)

    def tearDown(self):
        rmtree(self.tmp_path)

    def test_invalid_s3_seed_source(self):
        crawler = type("crawler", (object,), {})
        settings = Settings()
        settings.SEEDS_SOURCE = "invalid_url"
        crawler.settings = settings
        self.assertRaises(NotConfigured, S3SeedLoader, crawler)

    def test_process_start_requests(self):
        urls = [
            "https://www.example.com",
            "https://www.scrapy.org",
            "https://www.dmoz.org",
            "https://www.test.com",
        ]
        self.check_request_urls(urls)

    def test_s3_loader_ignores_non_txt_files(self):
        urls = []
        self.check_request_urls(urls, ".ini")

    def check_request_urls(self, urls, key_extension=".txt"):
        with Path(self.seed_path_1).open() as s1, Path(self.seed_path_2).open() as s2:
            conn = MockConnection()
            bucket = conn.create_bucket("some-bucket")
            bucket.add_key(f"seeds-folder/seeds1{key_extension}", s1)
            bucket.add_key(f"seeds-folder/seeds2{key_extension}", s2)

            def mocked_connect_s3(*args, **kwargs):
                return conn

            with mock.patch(
                "frontera.contrib.scrapy.middlewares.seeds.s3.connect_s3",
                side_effect=mocked_connect_s3,
            ):
                requests = self.seed_loader.process_start_requests(
                    None, Spider(name="spider")
                )
                self.assertEqual({r.url for r in requests}, set(urls))
