from frontera.settings import BaseSettings, DefaultSettings


class ScrapySettingsAdapter(BaseSettings):
    """
    Wrapps the frontera settings, falling back to scrapy and default settings
    """
    def __init__(self, crawler_settings):
        frontera_settings = crawler_settings.get('FRONTERA_SETTINGS', None)
        super(ScrapySettingsAdapter, self).__init__(module=frontera_settings)
        self._crawler_settings = crawler_settings or {}
        self._default_settings = DefaultSettings()

    def get(self, key, default_value=None):
        val = super(ScrapySettingsAdapter, self).get(key)
        if val is not None:
            return val

        val = self._crawler_settings.get(key)
        if val is not None:
            return val

        return self._default_settings.get(key, default_value)
