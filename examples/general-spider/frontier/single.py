from datetime import timedelta

BACKEND = "frontera.contrib.backends.sqlalchemy.revisiting.Backend"
SQLALCHEMYBACKEND_ENGINE = "sqlite:///url_storage.sqlite"
SQLALCHEMYBACKEND_ENGINE_ECHO = False
SQLALCHEMYBACKEND_DROP_ALL_TABLES = False
SQLALCHEMYBACKEND_CLEAR_CONTENT = False

SQLALCHEMYBACKEND_REVISIT_INTERVAL = timedelta(days=3)

DELAY_ON_EMPTY = 20.0
