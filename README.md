# Hyperon DAS

Hi! This package is a query engine API for Distributed AtomSpace (DAS). When is possible execute queries using **[Pattern Matcher](https://wiki.opencog.org/w/The_Pattern_Matcher)**

## Table of Contents
- [Hyperon DAS](#hyperon-das)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Using-pip](#using-pip)
    - [Using-Poetry](#using-poetry)
  - [Usage](#usage)
    - [Redis and MongoDB](#redis-and-mongodb)
        - [Create a client API](#create-a-client-api)
    - [In Memory](#in-memory)
  - [Tests](#tests)

## Installation

> Before you start, make sure you have [Python](https://www.python.org/) >= 3.8.5 and [Pip](https://pypi.org/project/pip/) installed on your system.

You can install and run this project using different methods. Choose the one that suits your needs.

### Using-pip

Run the following command to install the project using pip:: 

```bash
pip install hyperon-das
```

### Using-Poetry

If you prefer to manage your Python projects with [Poetry](https://python-poetry.org/), follow these steps:

1.    
    Install Poetry (if you haven't already):
    
    ```bash
    pip install poetry
    ``` 
    
2.  Clone the project repository:
    
    ```bash
    git clone git@github.com:singnet/das-query-engine.git
    cd das-query-engine
    ``` 
    
3.  Install project dependencies using Poetry:
    
    ```bash
    poetry install
    ``` 
    
4.  Activate the virtual environment created by Poetry:
    
    ```bash
    poetry shell
    ``` 

Now you can run the project within the Poetry virtual environment.

## Usage

So far we have two ways of making queries using the API. One that uses database persistence and another that doesn't. The way to create and execute the query is exactly the same, the only difference is when you need to instantiate the API class. Below you can see more details.

### Redis and MongoDB

If you want to use data persistence, you must have Redis and MongoDB running in your environment and you must have the following variables configured with their respective values:

*Example*:
```scheme
DAS_MONGODB_HOSTNAME=172.17.0.2
DAS_MONGODB_PORT=27017
DAS_MONGODB_USERNAME=mongo
DAS_MONGODB_PASSWORD=mongo
DAS_REDIS_HOSTNAME=127.0.0.1
DAS_REDIS_PORT=6379
```

**TIP**: You can change the values in the *environment* file, which is in the root directory and run the command below:

```bash
source environment
```

##### Create a client API

```python
from hyperon_das import DasAPI

api = DasAPI('redis_mongo')
```

### In Memory

This way you don't need anything just instantiate the class as shown below:


1. A simple query which is a `AND` operation on two links whose targets are variables.
	
    ```python
	from hyperon_das.pattern_matcher import And, Variable, Link

	V1 =  Variable("V1")
	V2 =  Variable("V2")
	V3 =  Variable("V3")

	expression =  And([
		Link("Inheritance",  ordered=True,  targets=[V1, V2]),
		Link("Inheritance",  ordered=True,  targets=[V2, V3])
	])

	resp = api.query(expression)
	
	print(resp)
	```

	```bash
	{{'V1': 'a1fb3a4de5c459bfa4bd87dc423019c3', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'bd497eb24420dd50fed5f3d2e6cdd7c1', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'e2d9b15ab3461228d75502e754137caa', 'V2': 'c90242e2dbece101813762cc2a83d726', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'd1ec11ec366a1deb24a079dc39863c68', 'V2': 'c90242e2dbece101813762cc2a83d726', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'fa77994f6835fad256902605a506c59c', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}, {'V1': 'c77b519f8ab36dfea8e2a532a7603d9a', 'V2': 'd1ec11ec366a1deb24a079dc39863c68', 'V3': 'c90242e2dbece101813762cc2a83d726'}, {'V1': '305e7d502a0ce80b94374ff0d79a6464', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'}}
	```

2. Add Node and And Link (It's possible only using [Hash Table](#in-memory)
	
	```python
	api.count_atoms() # (0, 0)
	
	nodes = [
	    {
	        'type': 'Reactome',
	        'name': 'Reactome:R-HSA-164843',
	    },
	    {
	        'type': 'Concept',
	        'name': 'Concept:2-LTR circle formation',
	    }
    ]
    
    for node in nodes:
	    api.add_node(node)
	
	api.count_atoms() # (2, 0)
	
	link = {
        'type': 'Evaluation',
        'targets': [
            {
	            'type': 'Predicate',
	            'name': 'Predicate:has_name'
	        },
            {
                'type': 'Evaluation',
                'targets': [
                    {
	                    'type': 'Predicate',
	                    'name': 'Predicate:has_name'
	                },
                    {
                        'type': 'Set',
                        'targets': [
                            {
                                'type': 'Reactome',
                                'name': 'Reactome:R-HSA-164843',
                            },
                            {
                                'type': 'Concept',
                                'name': 'Concept:2-LTR circle formation',
                            },
                        ]
                    },
                ],
            },
        ],
    }
    
    api.add_link(link)
    
    api.count_atoms() # (3, 3)
	```

	**Note1:** in this example I add 2 nodes and 1 a link, but in the end I have 3 nodes and 3 links. Therefore, it is possible to add nested links and as links are composed of nodes, if the link node doesn't exist in the system it's added.

	**Note2:** For these methods to work well, both nodes and links must be a dict with the structure shown above, i.e, for **nodes** you need to send, at least, the parameters `type` and `name` and for **links** `type` and `targets`

## Tests

You can run the command below to run the unit tests

```bash
make test-coverage
```