import pytest

from hyperon_das.context import Context


class TestContext:
    def test_creation(self):
        context_name = 'blah'
        context_handle = 'h'
        context = Context({'name': context_name, 'handle': context_handle}, [])
        assert context.name == context_name
        assert context.handle == context_handle
