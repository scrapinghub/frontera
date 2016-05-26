mock_variable = 'test'


class MockClass(object):

    val = 10

    def __init__(self, val):
        self.val = val


mock_instance = MockClass(5)


def mock_function():
    return 2
