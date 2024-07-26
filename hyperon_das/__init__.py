import sys

if sys.version_info < (3, 10):
    print('hyperon_das requires Python 3.10 or higher')
    sys.exit(1)

from hyperon_das.das import DistributedAtomSpace

__all__ = ['DistributedAtomSpace']

__version__ = '0.8.0'
