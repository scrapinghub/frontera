import pytest
from frontera.utils.misc import load_object, get_crc32, chunks


class TestGetCRC32(object):

    def test_bytes(self):
        assert get_crc32(b'example') == 1861000095

    def test_ascii_unicode(self):
        assert get_crc32(u'example') == 1861000095

    def test_non_ascii_unicode(self):
        assert get_crc32(u'example\u5000') == 1259721235


class TestChunks(object):

    def test_empty_list(self):
        assert list(chunks([], 1)) == []

    def test_multiple_length(self):
        assert list(chunks([1, 2, 3, 4, 5, 6], 2)) == [[1, 2], [3, 4], [5, 6]]

    def test_non_multiple_length(self):
        assert list(chunks([1, 2, 3, 4, 5, 6, 7, 8], 3)) == [[1, 2, 3], [4, 5, 6], [7, 8]]


class TestLoadObject(object):

    def test_load_class(self):
        obj = load_object('frontera.tests.mocks.load_objects.MockClass')
        assert obj.val == 10

    def test_load_instance(self):
        obj = load_object('frontera.tests.mocks.load_objects.mock_instance')
        assert obj.val == 5

    def test_load_variable(self):
        obj = load_object('frontera.tests.mocks.load_objects.mock_variable')
        assert obj == 'test'

    def test_load_function(self):
        obj = load_object('frontera.tests.mocks.load_objects.mock_function')
        assert obj() == 2

    def test_value_error(self):
        with pytest.raises(ValueError) as info:
            load_object('frontera')
        assert info.value.message == "Error loading object 'frontera': not a full path"

    def test_import_error(self):
        with pytest.raises(ImportError) as info:
            load_object('frontera.non_existent_module.object')
        assert info.value.message == ("Error loading object 'frontera.non_existent_module.object'"
                                      ": No module named non_existent_module")

    def test_name_error(self):
        with pytest.raises(NameError) as info:
            load_object('frontera.tests.mocks.load_objects.non_existent_object')
        assert info.value.message == ("Module 'frontera.tests.mocks.load_objects' doesn't define"
                                      " any object named 'non_existent_object'")
