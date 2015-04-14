import codecs

from scrapy.exceptions import NotConfigured

from frontera.contrib.scrapy.middlewares.seeds import SeedLoader


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
                seeds.append(clean_seed)
        return seeds

    def clean_seed(self, url):
        return url.strip('\t\n\r')
