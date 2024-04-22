import pytest
from hyperon_das.context import Context

class TestContext:
    def test_creation(self):
        c1 = Context("blah", "h")
        with pytest.raises(ValueError):
            c2 = Context("", "h")
        with pytest.raises(ValueError):
            c2 = Context("blah", "")
