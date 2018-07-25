from frontera.contrib.backends.sqlalchemy.components import DomainMetadata, DomainMetadataKV
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest import TestCase
import random
import string


def random_string(N):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))


class TestSqlAlchemyDomainMetadata(TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.session_cls = sessionmaker()
        self.session_cls.configure(bind=self.engine)
        DomainMetadataKV.__table__.create(bind=self.engine)

    def test_basic(self):
        dm = DomainMetadata(self.session_cls)
        value = {"someint": 1, "somefloat": 1, "someblob": b"bytes"}
        dm["test"] = value
        assert "test" in dm
        assert dm["test"] == value
        del dm["test"]
        assert "test" not in dm

        dm["test"] = 111
        assert "test" in dm
        assert dm["test"] == 111

    def test_many_items(self):
        dm = DomainMetadata(self.session_cls)
        for i in range(200):
            dm["key%d" % i] = random_string(10)

        for i in range(200):
            assert "key%d" % i in dm
            assert len(dm["key%d" % i]) == 10
            del dm["key%d" % i]

