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
## Tests

You can run the command below to run the unit tests

```bash
make test-unit
