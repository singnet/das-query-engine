# Hyperon DAS

Hi! This package is a query engine API for Distributed AtomSpace (DAS). When is possible execute queries using **[Pattern Matcher](https://wiki.opencog.org/w/The_Pattern_Matcher)**

## Table of Contents
- [Hyperon DAS](#hyperon-das)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Using-pip](#using-pip)
    - [Using-Poetry](#using-poetry)
  - [Usage](#usage)
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

It's very simple to use it. You just need to instantiate the class as shown below

```python
from hyperon_das import DistributedAtomSpace

das = DistributedAtomSpace()
```

Now you can enter Atoms and then make queries. See example below:

1. A simple query.
	
    ```python
    from hyperon_das import DistributedAtomSpace
    from hyperon_das.constants import QueryOutputFormat

    das = DistributedAtomSpace()

    das.count_atoms() # (0, 0)

    das.add_link({
        'type': 'Inheritance',
        'targets': [
            {'type': 'Concept', 'name': 'human'},
            {'type': 'Concept', 'name': 'mammal'}
        ],
    })

    das.add_link({
        'type': 'Inheritance',
        'targets': [
            {'type': 'Concept', 'name': 'monkey'},
            {'type': 'Concept', 'name': 'mammal'}
        ]
    })

    das.count_atoms() # (3, 2)

    query = {
        'atom_type': 'link',
        'type': 'Inheritance',
        'targets': [
            {'atom_type': 'variable', 'name': 'v1'},
            {'atom_type': 'node', 'type': 'Concept', 'name': 'mammal'},
        ]
    }

    query_params = {
        "toplevel_only": False,
        "return_type": QueryOutputFormat.ATOM_INFO,
    }

    resp = das.query(query, query_params)
	
	print(resp)
	```

	```bash
    [
        {
            'handle': 'c93e1e758c53912638438e2a7d7f7b7f',
            'targets': [
                {
                    'handle': 'af12f10f9ae2002a1607ba0b47ba8407',
                    'name': 'human',
                    'type': 'Concept'
                },
                {
                    'handle': 'bdfe4e7a431f73386f37c6448afe5840',
                    'name': 'mammal',
                    'type': 'Concept'
                }
            ],
            'template': ['Inheritance', 'Concept', 'Concept'],
            'type': 'Inheritance'
        },
        {
            'handle': 'f31dfe97db782e8cec26de18dddf8965',
            'targets': [
                {
                    'handle': '1cdffc6b0b89ff41d68bec237481d1e1',
                    'name': 'monkey',
                    'type': 'Concept'
                },
                {
                    'handle': 'bdfe4e7a431f73386f37c6448afe5840',
                    'name': 'mammal',
                    'type': 'Concept'
                }
            ],
            'template': ['Inheritance', 'Concept', 'Concept'],
            'type': 'Inheritance'
        }
    ]
	```


2. Add Node and And Link
	
	```python
    from hyperon_das import DistributedAtomSpace
    das = DistributedAtomSpace()
	
    das.count_atoms() # (0, 0)
	
	nodes = [
	    {
	        'type': 'Reactome',
	        'name': 'react',
	    },
	    {
	        'type': 'Concept',
	        'name': 'circle',
	    }
    ]
    
    for node in nodes:
	    das.add_node(node)
	
	das.count_atoms() # (2, 0)
    
	link = {
        'type': 'Evaluation',
        'targets': [
            {
	            'type': 'Predicate',
	            'name': 'pred'
	        },
            {
                'type': 'Evaluation',
                'targets': [
                    {
	                    'type': 'Predicate',
	                    'name': 'pred'
	                },
                    {
                        'type': 'Set',
                        'targets': [
                            {
                                'type': 'Reactome',
                                'name': 'react',
                            },
                            {
                                'type': 'Concept',
                                'name': 'circle',
                            },
                        ]
                    },
                ],
            },
        ],
    }

    das.add_link(link)
    
    das.count_atoms() # (3, 3)
	```

	**Note1:** in this example I add 2 nodes and 1 a link, but in the end I have 3 nodes and 3 links. Therefore, it is possible to add nested links and as links are composed of nodes, if the link node doesn't exist in the system it's added.

	**Note2:** For these methods to work well, both nodes and links must be a dict with the structure shown above, i.e, for **nodes** you need to send, at least, the parameters `type` and `name` and for **links** `type` and `targets`

3. You can add remote connections to make your queries

    ```python
    from hyperon_das import DistributedAtomSpace

    das = DistributedAtomSpace()

    host = '192.0.2.146'
    port = '8081'

    das.attach_remote(host=host, port=port)
    
    server = das.remote_das[0]

    server.count_atoms()
    server.query(query=...)
    ```

## Tests

You can run the command below to run the unit tests
test
```bash
make unit-tests
```
