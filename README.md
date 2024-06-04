# Hyperon DAS

A data manipulation API for Distributed Atomspace (DAS). It allows queries with pattern matching capabilities and traversal of the Atomspace hypergraph.

## Table of Contents
- [Hyperon DAS](#hyperon-das)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Installation](#installation)
    - [Using-pip](#using-pip)
    - [Using-Poetry](#using-poetry)
  - [Usage](#usage)
    - [Local DAS](#local-das)
    - [Remote DAS](#remote-das)
    - [Server DAS](#server-das)
  - [TraverseEngine](#traverseengine)
    - [Creating a TraverseEngine object](#creating-a-traverseengine-object)
    - [Traversal Methods](#traversal-methods)
    - [Parameters for Traversal Methods](#parameters-for-traversal-methods)
  - [Examples](#examples)
  - [Tests](#tests)
  - [Release Notes](#release-notes)

## Overview

For more details about the Distributed Atomspace and its components, you can see it on this page: [DAS Overview](https://singnet.github.io/das/das-overview)

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

You can instantiate DAS in three different ways:

### Local DAS
This is a local instance of DAS with default settings.

```python
from hyperon_das import DistributedAtomSpace

das = DistributedAtomSpace()
```

### Remote DAS

You need to specify the 'query_engine' parameter as 'remote' and pass 'host' and 'port' of the:

```python
from hyperon_das import DistributedAtomSpace

das = DistributedAtomSpace(query_engine='remote', host='0.0.0.0', port=1234)
```

### Server DAS
You'll need to specify the 'atomdb' parameter as 'redis_mongo' and pass the database parameters. The databases supported in this release are Redis and MongoDB. Therefore, the minimum expected parameters are:

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

## TraverseEngine

Introducing TraverseEngine! This API functionality can process some requests related to hypergraph traversal. In other words, it allows a given Atom to travel to it's neighborhood through adjacent links.

### Creating a TraverseEngine object

To create a TraverseEngine object, use the `get_traversal_cursor` method, which expects a handle as a starting point for the traversal.

Example:

```python
from hyperon_das import DistributedAtomSpace

das = DistributedAtomSpace()

traverse_engine = das.get_traversal_cursor(handle='12345')
```

### Traversal Methods

The TraverseEngine provides some methods for graph traversal:

1. **get()**: Return the current cursor.
2. **get_links(kwargs)**: Return any links having current cursor as one of its targets, i.e. any links pointing to cursor.
3. **get_neighbors(kwargs)**: Returns the set formed by all targets of all links that point to the current cursor. In other words, the set of “neighbors” of the current cursor.
4. **follow_link(kwargs)**: Updates the current cursor by following a link and selecting one of its targets, randomly.
5. **goto(handle)**: Reset the current cursor to the passed handle.

### Parameters for Traversal Methods

Various parameters can be passed to the traversal methods to filter the results. For example:

1. **link_type=XXX**: Filters to contain only links whose named_type == XXX.
2. **cursor_position=N**: Filters the response so that only links with the current cursor at the nth position of their target are returned.
3. **target_type=XXX**:  Filters to only contain links whose at least one of the targets has named_type == XXX.
4. **filter=F**: F is a function or a tuple of functions That is used to filter the results after applying all other filters. F should expect a dict (the atom document) and return True if and only if this atom should be kept. It's possible to apply custom filters to both Links and Neighbors. See bellow:                 

Possible use cases to filter parameter:

    a. traverse.get_neighbors(..., filter=custom_filter)
        -> The custom_filter will be applied to Links
    b. traverse.get_neighbors(..., filter=(custom_filter1, custom_filter2))
        -> The custom_filter1 will be applied to Links and custom_filter2 will be applied Targets
    c. traverse.get_neighbors(..., filter=(None, custom_filter2))
        -> The custom_filter2 will only be applied to Targets. This way there is no filter to Links
    d. traverse.get_neighbors(..., filter=(custom_filter1, None))
        -> The custom_filter1 will be applied to Links. This case is equal case `a`

## Examples

For the examples using the API, see the [User's Guide](https://singnet.github.io/das/das-users-guide) 


## Tests

In the main project directory, you can run the command below to run the unit and integration tests

```bash
make unit-tests
```

```bash
make integration-tests
```

## Release Notes

[DAS Query Engine Releases](https://github.com/singnet/das-query-engine/releases)
