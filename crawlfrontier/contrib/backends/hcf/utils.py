import os

from crawlfrontier.exceptions import NotConfigured


class ScrapyStatsCollectorWrapper(object):
    """
    Scrapy StatsCollector wrapper class.
    Calls StatsCollector methods if its initialized with one. If not, use dummy methods.
    """
    def __init__(self, stats):
        self._stats = stats

    def __getattr__(self, name):
        def dummy_method(*args, **kwargs):
            pass
        stats = self.__dict__.get('_stats', None)
        if stats:
            return getattr(stats, name)
        else:
            return dummy_method


def get_scrapy_crawler(data):
    return data.get('crawler', None)


def get_scrapy_stats(data):
    crawler = get_scrapy_crawler(data)
    return crawler.stats if crawler else None


def get_scrapy_settings(data):
    crawler = get_scrapy_crawler(data)
    return crawler.settings if crawler else None


class ParameterManager(object):
    """
    Try to get values from the following sources:
        - scrapy conf
        - scrapy settings
        - frontier settings
    """
    def __init__(self, manager):
        self.scrapy_conf = get_project_conf()
        self.scrapy_settings = get_scrapy_settings(manager.extra)
        self.frontier_settings = manager.settings

    def get_from_all(self, conf_name, settings_name, default=None, required=False):
        """
        Try to get values from all the sources:
            - scrapy conf
            - scrapy settings
            - frontier settings
        If not found default is returned
        """
        value = self.get_from_scrapy_conf(conf_name, default) or \
                self.get_from_all_settings(settings_name, default) or \
                default
        return self._return_value('%s/%s' % (conf_name, settings_name), value, required, 'anywhere')

    def get_from_all_settings(self, name, default=None, required=False):
        """
        Try to get values from all the settings:
            - scrapy settings
            - frontier settings
        If not found default is returned
        """
        value = self.get_from_scrapy_settings(name, default) or \
                self.get_from_frontier_settings(name, default) or \
                default
        return self._return_value(name, value, required, 'in any settings')

    def get_from_scrapy_conf(self, name, default=None, required=False):
        value = self.scrapy_conf.get(name, default)
        return self._return_value(name, value, required, 'in scrapy conf')

    def get_from_scrapy_settings(self, name, default=None, required=False):
        value = self.scrapy_settings.get(name, default)
        return self._return_value(name, value, required, 'in scrapy settings')

    def get_from_frontier_settings(self, name, default=None, required=False):
        value = self.frontier_settings.get(name, default)
        return self._return_value(name, value, required, 'in frontier settings')

    def _return_value(self, name, value, required, source):
        if not value and required:
            raise NotConfigured("Missing required value '%s' not found %s" % (name, source))
        return value


def get_project_conf():
    """
    Gets hs auth and project id, from following sources in following order of precedence:
    - hworker.bot.hsref
    - environment variables
    - scrapy.cfg files

    in order to allow to use codes that needs HS or dash API, either locally or from scrapinghub, correctly configured
    """
    conf = {
        'project_id': os.environ.get('PROJECT_ID'),
        'auth': os.environ.get('SHUB_APIKEY') + ':' if os.environ.get('SHUB_APIKEY') else None
    }
    try:
        from hworker.bot.hsref import hsref
        conf = {'project_id': hsref.projectid, 'auth': hsref.auth}
    except Exception, e:
        pass

    cfg = {}
    try:
        from scrapy.utils.conf import get_config
        cfg = dict(get_config().items('deploy'))
    except:
        pass

    try:
        if conf['project_id'] is None:
            conf['project_id'] = cfg.get('project')
    except:
        pass

    try:
        if conf['auth'] is None:
            username = cfg.get('username')
            conf['auth'] = '%s:' % username if username else None
    except:
        pass

    return conf
