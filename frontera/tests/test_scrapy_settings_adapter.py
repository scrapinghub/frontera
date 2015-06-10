# -*- coding: utf-8 -*-

from frontera.contrib.scrapy.settings_adapter import ScrapySettingsAdapter


def test_fallsback_to_crawler_settings():
    settings = ScrapySettingsAdapter({'DELAY_ON_EMPTY': 10})
    assert settings.get('DELAY_ON_EMPTY') == 10


def test_frontera_settings_have_precedence_over_crawler_settings():
    crawler_settings = {'MAX_REQUESTS': 10,
                        'FRONTERA_SETTINGS': 'frontera.tests.scrapy_spider.frontera.settings'}
    settings = ScrapySettingsAdapter(crawler_settings)
    assert settings.get('MAX_REQUESTS') == 5
