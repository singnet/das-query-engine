# Hyperon DAS

A data manipulation API for Distributed Atomspace (DAS). It allows queries with pattern matching capabilities and traversal of the Atomspace hypergraph.

## References and Guides

- Details about the Distributed Atomspace and its components: [DAS Overview](https://singnet.github.io/das/das-overview)
- PYPI Library package: [hyperon-das](https://pypi.org/project/hyperon-das/)
- Examples using the API: [User's Guide](https://singnet.github.io/das/das-users-guide) 
- Release notes: [DAS Query Engine Releases](https://github.com/singnet/das-query-engine/releases)

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

## Tests

In the main project directory, you can run the command below to run the unit tests

```bash
make unit-tests
```

You can do the same to run integration tests

```bash
make integration-tests
```

The integration tests use a remote testing server hosted on Vultr, at the address `45.63.85.59`, port `8080`. The loaded knowledge base is the animal base, which contains the Nodes and Links listed below:

```text
(: Similarity Type)
(: Concept Type)
(: Inheritance Type)
(: "human" Concept)
(: "monkey" Concept)
(: "chimp" Concept)
(: "snake" Concept)
(: "earthworm" Concept)
(: "rhino" Concept)
(: "triceratops" Concept)
(: "vine" Concept)
(: "ent" Concept)
(: "mammal" Concept)
(: "animal" Concept)
(: "reptile" Concept)
(: "dinosaur" Concept)
(: "plant" Concept)
(Similarity "human" "monkey")
(Similarity "human" "chimp")
(Similarity "chimp" "monkey")
(Similarity "snake" "earthworm")
(Similarity "rhino" "triceratops")
(Similarity "snake" "vine")
(Similarity "human" "ent")
(Inheritance "human" "mammal")
(Inheritance "monkey" "mammal")
(Inheritance "chimp" "mammal")
(Inheritance "mammal" "animal")
(Inheritance "reptile" "animal")
(Inheritance "snake" "reptile")
(Inheritance "dinosaur" "reptile")
(Inheritance "triceratops" "dinosaur")
(Inheritance "earthworm" "animal")
(Inheritance "rhino" "mammal")
(Inheritance "vine" "plant")
(Inheritance "ent" "plant")
(Similarity "monkey" "human")
(Similarity "chimp" "human")
(Similarity "monkey" "chimp")
(Similarity "earthworm" "snake")
(Similarity "triceratops" "rhino")
(Similarity "vine" "snake")
(Similarity "ent" "human")
```
