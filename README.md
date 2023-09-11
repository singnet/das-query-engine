# Hyperon DAS

Query Engine API for Distributed AtomSpace and Pattern Matcher.

## Installation

This packaage requires [python](https://www.python.org/) >= 3.8.5 to run.

```
pip install hyperon-das
```

## Prepare environment

**1. Redis and MongoDB**
You must have Redis and MongoDB running in your environment
**2. Environments Variables**
You must have the following variables set in your environment with their respective values:

```
DAS_MONGODB_HOSTNAME=172.17.0.2
DAS_MONGODB_PORT=27017
DAS_MONGODB_USERNAME=mongo
DAS_MONGODB_PASSWORD=mongo
DAS_REDIS_HOSTNAME=127.0.0.1
DAS_REDIS_PORT=6379
```

## Usage

#### Create a client API

```python
from hyperon_das import DasAPI

api_client = DasAPI('redis_mongo')
```

### Example

```python
from hyperon_das.pattern_matcher import And, Variable, Link

V1 = Variable("V1")
V2 = Variable("V2")
V3 = Variable("V3")

expression = And([
    Link("Inheritance", ordered=True, targets=[V1, V2]),
    Link("Inheritance", ordered=True, targets=[V2, V3])
])

resp = api_client.query(expression)

print(resp)
```

```bash
{{'V1': 'a1fb3a4de5c459bfa4bd87dc423019c3', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'bd497eb24420dd50fed5f3d2e6cdd7c1', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'e2d9b15ab3461228d75502e754137caa', 'V2': 'c90242e2dbece101813762cc2a83d726', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'd1ec11ec366a1deb24a079dc39863c68', 'V2': 'c90242e2dbece101813762cc2a83d726', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'fa77994f6835fad256902605a506c59c', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'c77b519f8ab36dfea8e2a532a7603d9a', 'V2': 'd1ec11ec366a1deb24a079dc39863c68', 'V3': 'c90242e2dbece101813762cc2a83d726'}, {'V1': '305e7d502a0ce80b94374ff0d79a6464', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}}
```