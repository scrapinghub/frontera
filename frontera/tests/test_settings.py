# -*- coding: utf-8 -*-

from frontera.settings import Settings, BaseSettings


def test_settings_on_a_python_module_are_loaded():
    settings = Settings('frontera.tests.scrapy_spider.frontera.settings')
    assert settings.get('MAX_REQUESTS') == 5


def test_settings_passed_as_attributes_can_be_found():
    settings = Settings(attributes={'SETTING': 'value'})
    assert settings.get('SETTING') == 'value'


def test_fallsback_to_frontera_default_settings():
    settings = Settings()
    assert settings.get('MAX_NEXT_REQUESTS') == 0


def test_allows_settings_to_be_accessed_by_attribute():
    settings = Settings()
    assert settings.MAX_NEXT_REQUESTS == 0


def test_settings_attributes_can_be_assigned():
    settings = Settings()
    settings.NEW_ATTRIBUTE = 10
    assert settings.NEW_ATTRIBUTE == 10


def test_object_from_loads_settings_from_a_module():
    module = 'frontera.tests.scrapy_spider.frontera.settings'
    settings = BaseSettings.object_from(module)
    assert settings.get('MAX_REQUESTS') == 5


def test_new_instance_copies_the_given_instance():
    settings = Settings()
    new_instance = BaseSettings.object_from(settings)
    assert new_instance.MAX_NEXT_REQUESTS == 0
    assert type(new_instance) == Settings
