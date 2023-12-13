# Hyperon DAS

Hi! This package is a query engine API for Distributed AtomSpace (DAS). When is possible execute queries using **[Pattern Matcher](https://wiki.opencog.org/w/The_Pattern_Matcher)**

## Table of Contents
- [Hyperon DAS](#hyperon-das)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Using-pip](#using-pip)
    - [Using-Poetry](#using-poetry)
  - [Usage](#usage)
    - [Local](#local)
    - [Remote](#remote)
    - [Server](#server)
  - [Examples](#examples)
    - [Local DAS](#local-das)
    - [Remote DAS](#remote-das)
      - [Local Scope](#local-scope)
      - [Remote scope](#remote-scope)
      - [Remote scope synchronized with local Atoms](#remote-scope-synchronized-with-local-atoms)
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

You can instantiate DAS in three different ways. see below:

### Local
This is a local instance of DAS with default settings.

```python
from hyperon_das import DistributedAtomSpace

das = DistributedAtomSpace()
```

### Remote

To create a remote DAS, you need to specify the 'query_engine' parameter as 'remote' and pass the machine, 'host' and 'port' parameters. See below how to do this:

```python
from hyperon_das import DistributedAtomSpace

das = DistributedAtomSpace(query_engine='remote', host='0.0.0.0', port=1234)
```

### Server
To create a DAS server, you will need to specify the 'atomdb' parameter as 'redis_mongo' and pass the database parameters. The databases supported in this release are Redis and MongoDB. Therefore, the minimum expected parameters are:

- mongo_hostname
- mongo_port
- mongo_username
- mongo_password
- redis_hostname
- redis_port

but it is possible to pass other configuration parameters:

- mongo_tls_ca_file
- redis_username
- redis_password
- redis_cluster
- redis_ssl

```python
from hyperon_das import DistributedAtomSpace

das = DistributedAtomSpace(
    atomdb='redis_mongo',
    mongo_hostname='127.0.0.2',
    mongo_port=27017,
    mongo_username='mongo',
    mongo_password='mongo',
    redis_hostname='127.0.0.1',
    redis_port=6379
)
```

## Examples

### Local DAS

This way it is only possible to make queries in your local memory with the Atoms that you placed in the DAS. See bellow:

```python
from hyperon_das import DistributedAtomSpace

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
    "toplevel_only": False
}

resp = das.query(query, query_params)

print(resp)
```
```bash
[
    {
        "handle": "c93e1e758c53912638438e2a7d7f7b7f",
        "type": "Inheritance",
        "template": ["Inheritance", "Concept", "Concept"],
        "targets": [
            {
                "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                "type": "Concept",
                "name": "human",
            },
            {
                "handle": "bdfe4e7a431f73386f37c6448afe5840",
                "type": "Concept",
                "name": "mammal",
            },
        ],
    },
    {
        "handle": "f31dfe97db782e8cec26de18dddf8965",
        "type": "Inheritance",
        "template": ["Inheritance", "Concept", "Concept"],
        "targets": [
            {
                "handle": "1cdffc6b0b89ff41d68bec237481d1e1",
                "type": "Concept",
                "name": "monkey",
            },
            {
                "handle": "bdfe4e7a431f73386f37c6448afe5840",
                "type": "Concept",
                "name": "mammal",
            },
        ],
    },
]
```

### Remote DAS

This way it'ss possible to make queries in your local memory and on the remote machine that you need to pass during the creation of DAS instance. See below:

```python
from hyperon_das import DistributedAtomSpace

das = DistributedAtomSpace(query_engine='remote', host='192.32.11.45', port=9000)
```

In the query method is possible pass query_scope parameter with four available values. This specifying whether you want to make the query local, remote, local and remote or synchronize and remote. If you don't pass the default is "remote_only"

1. "local_only"
- Only local query 
2. "remote_only"
- Only remote query
1. "local_and_remote"
- This type is not available yet. So, this will raise an exception
1. "synchronous_update"
- Before make query it will commit your local changes and then make the remote query


#### Local Scope

```python
query = {
    'atom_type': 'link',
    'type': 'Inheritance',
    'targets': [
        {'atom_type': 'node', 'type': 'Concept', 'name': 'humana'},
        {'atom_type': 'node', 'type': 'Concept', 'name': 'mammala'},
    ]
}

query_params = {
    "toplevel_only": False,
    "query_scope": "local_only"
}

resp = das.query(query, query_params)
print(resp)
```
```bash
[]
```

#### Remote scope

```python
query = {
    'atom_type': 'link',
    'type': 'Inheritance',
    'targets': [
        {'atom_type': 'node', 'type': 'Concept', 'name': 'human'},
        {'atom_type': 'node', 'type': 'Concept', 'name': 'mammal'},
    ]
}

query_params = {
    "toplevel_only": False
}

resp = das.query(query, query_params)

print(resp)
```
```bash
[
    {
        "handle": "c93e1e758c53912638438e2a7d7f7b7f",
        "type": "Inheritance",
        "template": ["Inheritance", "Concept", "Concept"],
        "targets": [
            {
                "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                "type": "Concept",
                "name": "human",
            },
            {
                "handle": "bdfe4e7a431f73386f37c6448afe5840",
                "type": "Concept",
                "name": "mammal",
            },
        ],
    }
]
```

#### Remote scope synchronized with local Atoms

```python

# Add a local Link
das.add_link({
    'type': 'Inheritance',
    'targets': [
        {'type': 'Concept', 'name': 'monkey'},
        {'type': 'Concept', 'name': 'mammal'}
    ]
})

query = {
    'atom_type': 'link',
    'type': 'Inheritance',
    'targets': [
        {'atom_type': 'node', 'type': 'Concept', 'name': 'human'},
        {'atom_type': 'node', 'type': 'Concept', 'name': 'mammal'},
    ]
}

query_params = {
    "toplevel_only": False,
    "query_scope": "synchronous_update"
}

resp = das.query(query, query_params)

print(resp)
```
```bash
[
    {
        "handle": "c93e1e758c53912638438e2a7d7f7b7f",
        "type": "Inheritance",
        "template": ["Inheritance", "Concept", "Concept"],
        "targets": [
            {
                "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                "type": "Concept",
                "name": "human",
            },
            {
                "handle": "bdfe4e7a431f73386f37c6448afe5840",
                "type": "Concept",
                "name": "mammal",
            },
        ],
    },
    {
        "handle": "f31dfe97db782e8cec26de18dddf8965",
        "type": "Inheritance",
        "template": ["Inheritance", "Concept", "Concept"],
        "targets": [
            {
                "handle": "1cdffc6b0b89ff41d68bec237481d1e1",
                "type": "Concept",
                "name": "monkey",
            },
            {
                "handle": "bdfe4e7a431f73386f37c6448afe5840",
                "type": "Concept",
                "name": "mammal",
            },
        ],
    },
]
```

## Tests

You can run the command below to run the unit tests

```bash
make test-unit
```