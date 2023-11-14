import pytest
from hyperon_das.cache import ListIterator, ProductIterator
from hyperon_das.utils import Assignment


class TestCache:

    def test_list_iterator(self):

        iterator = ListIterator(None)
        for element in iterator:
            assert False

        iterator = ListIterator([])
        for element in iterator:
            assert False

        iterator = ListIterator([
            ([{"id": 1}], Assignment()),
        ])
        expected = [1]
        count = 0
        for element in iterator:
            assert element[0][0]["id"] == expected[count]
            count += 1
        assert count == len(expected)

        iterator = ListIterator([
            ([{"id": 1}], Assignment()),
            ([{"id": 2}], Assignment()),
        ])
        expected = [1, 2]
        count = 0
        for element in iterator:
            assert element[0][0]["id"] == expected[count]
            count += 1
        assert count == len(expected)

        iterator = ListIterator([
            ([{"id": 1}], Assignment()),
            ([{"id": 2}], Assignment()),
            ([{"id": 2}], Assignment()),
        ])
        expected = [1, 2, 2]
        count = 0
        for element in iterator:
            assert element[0][0]["id"] == expected[count]
            count += 1
        assert count == len(expected)

        iterator = ListIterator([([{"id": 1}], Assignment()),])
        assert not iterator.is_empty()
        iterator = ListIterator([None])
        assert not iterator.is_empty()
        iterator = ListIterator([])
        assert iterator.is_empty()
        iterator = ListIterator(None)
        assert iterator.is_empty()

    def test_product_iterator(self):

        ln = None
        l0 = []
        l1 = [1, 2, 3]
        l2 = [4]
        l3 = [5, 6]
        l4 = [7, 8]

        li1 = ListIterator(l1)
        li3 = ListIterator(l3)
        iterator = ProductIterator([li1, li3])
        assert not iterator.is_empty()
        assert iterator.get() == (1, 5)
        expected = [(1, 5), (1, 6), (2, 5), (2, 6), (3, 5), (3, 6)]
        count = 0
        for element in iterator:
            assert element == expected[count]
            assert iterator.get() == expected[count]
            count += 1
        assert not iterator.is_empty()
        with pytest.raises(StopIteration):
            assert iterator.get()

        li3 = ListIterator(l3)
        li4 = ListIterator(l4)
        iterator = ProductIterator([li3, li4])
        expected = [(5, 7), (5, 8), (6, 7), (6, 8)]
        count = 0
        for element in iterator:
            assert element == expected[count]
            count += 1
        assert not iterator.is_empty()

        li1 = ListIterator(l1)
        li2 = ListIterator(l2)
        li3 = ListIterator(l3)
        iterator = ProductIterator([li1, li2, li3])
        expected = [(1, 4, 5), (1, 4, 6), (2, 4, 5), (2, 4, 6), (3, 4, 5), (3, 4, 6)]
        count = 0
        for element in iterator:
            assert element == expected[count]
            count += 1
        with pytest.raises(StopIteration):
            assert iterator.get()

        for arg in [[ln, l1], [ln, l1, l2], [ln]]:
            iterator = ProductIterator([ListIterator(v) for v in arg])
            assert iterator.is_empty()
            for element in iterator:
                assert False
            assert iterator.is_empty()

        for arg in [[l0, l1], [l0, l1, l2], [l0]]:
            iterator = ProductIterator([ListIterator(v) for v in arg])
            assert iterator.is_empty()
            for element in iterator:
                assert False
            assert iterator.is_empty()
