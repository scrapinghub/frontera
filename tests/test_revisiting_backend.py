import pytest

sqlalchemy = pytest.importorskip("sqlalchemy.engine")

from datetime import timedelta
from time import sleep

from frontera.utils.tester import FrontierTester
from tests.backends import TEST_SITES, BackendSequenceTest


class RevisitingFrontierTester(FrontierTester):
    def run(self, add_all_pages=False):
        if not self.frontier.auto_start:
            self.frontier.start()
        if not add_all_pages:
            self._add_seeds()
        else:
            self._add_all()
        while not self.frontier.finished:
            result = self._run_iteration()
            self.sequence.append(result)
            requests, iteration, dl_info = result
            if self.downloader_simulator.idle():
                sleep(0.5)
            if iteration == 5:
                break
        self.frontier.stop()


class RevisitingBackendTest(BackendSequenceTest):
    def get_settings(self):
        settings = super().get_settings()
        settings.set("SQLALCHEMYBACKEND_REVISIT_INTERVAL", timedelta(seconds=2))
        settings.SQLALCHEMYBACKEND_ENGINE = "sqlite:///:memory:"
        return settings

    @pytest.mark.parametrize(
        ("site_list", "max_next_requests"),
        [
            ("SITE_01", 5),
            ("SITE_02", 10),
        ],
    )
    def test_sequence(self, site_list, max_next_requests):
        sequence = self.get_url_sequence(
            site_list=TEST_SITES[site_list],
            max_next_requests=max_next_requests,
            frontier_tester=RevisitingFrontierTester,
        )
        seen = set()
        for url in sequence:
            if url in seen:
                return
            seen.add(url)

        raise AssertionError("None of the URLs were revisted")
