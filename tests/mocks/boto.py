import six


class Content:

    def __init__(self, obj):
        self.obj = obj

    def split(self):
        return self.obj


class MockKey:

    def __init__(self, name, data):
        self.name = name
        self.content = Content(data)

    def get_contents_as_string(self, *args, **kwargs):
        return self.content


class MockBucket:

    def __init__(self):
        self.keys = {}

    def list(self, prefix):
        return [key for name, key in self.keys.items() if name.startswith(prefix)]

    def add_key(self, name, data):
        if name in self.keys:
            raise Exception('key: %s already exists' % name)
        self.keys[name] = MockKey(name, data)


class MockConnection:

    def __init__(self):
        self.buckets = {}

    def get_bucket(self, bucket_name):
        try:
            return self.buckets[bucket_name]
        except:
            raise Exception('Bucket: %s not found' % bucket_name)

    def create_bucket(self, name):
        if name in self.buckets:
            raise Exception('Bucket: %s already exists' % name)
        self.buckets[name] = MockBucket()
        return self.buckets[name]
