import hashlib

import pytest

from frontera.utils.misc import chunks, get_crc32, load_object, to_signed32


class TestGetCRC32:
    def test_bytes(self):
        assert get_crc32(b"example") == 1861000095

    def test_ascii_unicode(self):
        assert get_crc32("example") == 1861000095

    def test_non_ascii_unicode(self):
        assert get_crc32("example\u5000") == 1259721235

    def test_non_ascii_bytes(self):
        assert get_crc32("example\u5000".encode()) == 1259721235

    def test_negative_crc32(self):
        assert get_crc32(b"1") == -2082672713

    def test_crc32_range(self):
        left, right = -(2**31), 2**31 - 1
        for x in range(10000):
            bytestr = hashlib.md5(str(x).encode("ascii")).hexdigest()
            assert left <= get_crc32(bytestr) <= right
        for x in [left, left + 1, right - 1, right, right + 1, 2**32 - 2, 2**32 - 1]:
            assert left <= to_signed32(x) <= right


class TestChunks:
    def test_empty_list(self):
        assert list(chunks([], 1)) == []

    def test_multiple_length(self):
        assert list(chunks([1, 2, 3, 4, 5, 6], 2)) == [[1, 2], [3, 4], [5, 6]]

    def test_non_multiple_length(self):
        assert list(chunks([1, 2, 3, 4, 5, 6, 7, 8], 3)) == [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8],
        ]


class TestLoadObject:
    def test_load_class(self):
        obj = load_object("tests.mocks.load_objects.MockClass")
        assert obj.val == 10

    def test_load_instance(self):
        obj = load_object("tests.mocks.load_objects.mock_instance")
        assert obj.val == 5

    def test_load_variable(self):
        obj = load_object("tests.mocks.load_objects.mock_variable")
        assert obj == "test"

    def test_load_function(self):
        obj = load_object("tests.mocks.load_objects.mock_function")
        assert obj() == 2

    def test_value_error(self):
        with pytest.raises(ValueError) as info:
            load_object("frontera")
        assert str(info.value) == "Error loading object 'frontera': not a full path"

    def test_import_error(self):
        with pytest.raises(ImportError) as info:
            load_object("frontera.non_existent_module.object")
        assert str(info.value) == (
            "Error loading object 'frontera.non_existent_module.object'"
            ": No module named 'frontera.non_existent_module'"
        )

    def test_name_error(self):
        with pytest.raises(NameError) as info:
            load_object("tests.mocks.load_objects.non_existent_object")
        assert str(info.value) == (
            "Module 'tests.mocks.load_objects' doesn't define"
            " any object named 'non_existent_object'"
        )
