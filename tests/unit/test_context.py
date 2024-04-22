import pytest

from hyperon_das.context import Context


class TestContext:
    def test_creation(self):
        Context("blah", "h")
        with pytest.raises(ValueError):
            Context("", "h")
        with pytest.raises(ValueError):
            Context("blah", "")
