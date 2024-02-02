from typing import Any, Dict, List, Optional, Tuple, Union

from hyperon_das_atomdb import AtomDB, AtomDoesNotExist
from hyperon_das_atomdb.adapters import InMemoryDB, RedisMongoDB
from hyperon_das_atomdb.exceptions import InvalidAtomDB

from hyperon_das.cache import QueryAnswerIterator
from hyperon_das.exceptions import (
    GetTraversalCursorException,
    InvalidDASParameters,
    InvalidQueryEngine,
)
from hyperon_das.logger import logger
from hyperon_das.query_engines import LocalQueryEngine, RemoteQueryEngine
from hyperon_das.traverse_engines import TraverseEngine
from hyperon_das.utils import Assignment


class DistributedAtomSpace:
    def __init__(self, **kwargs: Optional[Dict[str, Any]]) -> None:
        atomdb_parameter = kwargs.get('atomdb', 'ram')
        query_engine_parameter = kwargs.get('query_engine', 'local')

        if atomdb_parameter == "ram":
            self.backend = InMemoryDB()
        elif atomdb_parameter == "redis_mongo":
            self.backend = RedisMongoDB(**kwargs)
            if query_engine_parameter != "local":
                raise InvalidDASParameters(
                    message="'redis_mongo' backend requires local query engine ('query_engine=local')"
                )
        else:
            raise InvalidAtomDB(message="Invalid AtomDB type. Choose either 'ram' or 'redis_mongo'")

        if query_engine_parameter == 'local':
            self.query_engine = LocalQueryEngine(self.backend, kwargs)
            logger().info('Initialized local Das')
        elif query_engine_parameter == "remote":
            self.query_engine = RemoteQueryEngine(self.backend, kwargs)
            logger().info('Initialized remote Das')
        else:
            raise InvalidQueryEngine(
                message='The possible values are: `local` or `remote`',
                details=f'query_engine={query_engine_parameter}',
            )

    def get_atom(self, handle: str, **kwargs) -> Union[Dict[str, Any], None]:
        """
        Retrieve information about an Atom using its handle.

        This method retrieves information about an Atom from the database
        based on the provided handle.

        Args:
            handle (str): The unique handle of the atom.

        Returns:
            Union[Dict, None]: A dictionary containing detailed Atom information

        Raises:
            AtomDoesNotExist: If invalid parameter is provided.

        Example:
            >>> result = das.get_atom(handle="af12f10f9ae2002a1607ba0b47ba8407")
            >>> print(result)
            {
                'handle': 'af12f10f9ae2002a1607ba0b47ba8407',
                'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
                'name': 'human',
                'named_type': 'Concept'
            }
        """
        return self.query_engine.get_atom(handle, **kwargs)

    def get_node(self, node_type: str, node_name: str) -> Union[Dict[str, Any], None]:
        """
        This method retrieves information about a Node from the database
        based on its type and name.

        Args:
            node_type (str): The type of the node being queried.
            node_name (str): The name of the specific node being queried.

        Returns:
            Union[Dict, None]: A dictionary containing detailed node information

        Raises:
            NodeDoesNotExist: If invalid parameters are provided.

        Example:
            >>> result = das.get_node(
                    node_type='Concept',
                    node_name='human'
                )
            >>> print(result)
            {
                'handle': 'af12f10f9ae2002a1607ba0b47ba8407',
                'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
                'name': 'human',
                'named_type': 'Concept'
            }
        """
        return self.query_engine.get_node(node_type, node_name)

    def get_link(self, link_type: str, link_targets: List[str]) -> Union[Dict[str, Any], None]:
        """
        This method retrieves information about a link from the database based on
        type with given targets.

        Args:
            link_type (str): The type of the link being queried.
            link_targets (List[str]): A list of target identifiers that the link is associated with.

        Returns:
            Union[Dict, None]: A dictionary containing detailed link information

        Raises:
            LinkDoesNotExist: If invalid parameters are provided.

        Note:
            If the specified link or targets do not exist, the method returns None.

        Example:
            >>> human_handle = das.get_node_handle('Concept', 'human')
            >>> monkey_handle = das.get_node_handle('Concept', 'monkey')
            >>> result = das.get_link(
                    link_type='Similarity',
                    link_targets=[human_handle, monkey_handle],
                )
            >>> print(result)
            {
                'handle': 'bad7472f41a0e7d601ca294eb4607c3a',
                'composite_type_hash': 'ed73ea081d170e1d89fc950820ce1cee',
                'is_toplevel': True,
                'composite_type': [
                    'a9dea78180588431ec64d6bc4872fdbc',
                    'd99a604c79ce3c2e76a2f43488d5d4c3',
                    'd99a604c79ce3c2e76a2f43488d5d4c3'
                ],
                'named_type': 'Similarity',
                'named_type_hash': 'a9dea78180588431ec64d6bc4872fdbc',
                'targets': [
                    'af12f10f9ae2002a1607ba0b47ba8407',
                    '1cdffc6b0b89ff41d68bec237481d1e1'
                ]
            }

        """
        return self.query_engine.get_link(link_type, link_targets)

    def get_links(
        self, link_type: str, target_types: List[str] = None, link_targets: List[str] = None
    ) -> Union[List[str], List[Dict]]:
        """
        Retrieve information about Links based on specified criteria.

        This method retrieves information about links from the database based on the provided criteria.
        The criteria includes the link type, and can include target types and specific target identifiers.
        The retrieved links information can be presented in different output formats as specified
        by the output_format parameter.

        Args:
            link_type (str): The type of links being queried.
            target_types (List[str], optional): The type(s) of targets being queried. Defaults to None.
            link_targets (List[str], optional): A list of target identifiers that the links are associated with.
                Defaults to None.

        Returns:
            Union[List[str], List[Dict]]: A list of dictionaries containing detailed information of the links

        Example:
            >>> result = das.get_links(
                    link_type='Similarity',
                    target_types=['Concept', 'Concept']
                )
            >>> print(result)
            [
                {
                    'handle': 'a45af31b43ee5ea271214338a5a5bd61',
                    'type': 'Similarity',
                    'template': ['Similarity', 'Concept', 'Concept'],
                    'targets': [...]
                },
                {
                    'handle': '2d7abd27644a9c08a7ca2c8d68338579',
                    'type': 'Similarity',
                    'template': ['Similarity', 'Concept', 'Concept'],
                    'targets': [...]
                },
                ...
            ]
        """
        return self.query_engine.get_links(link_type, target_types, link_targets)

    def get_incoming_links(self, atom_handle: str, **kwargs) -> List[Union[Dict[str, Any], str]]:
        """Retrieve all links pointing to Atom

        Args:
            atom_handle (str): The unique handle of the atom
            handles_only (bool, optional): If True return only handles. Defaults to False.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing detailed information of the atoms
            or a list of strings containing the atom handles
        """
        return self.query_engine.get_incoming_links(atom_handle, **kwargs)

    def count_atoms(self) -> Tuple[int, int]:
        """
        This method is useful for returning the count of atoms in the database.
        It's also useful for ensuring that the knowledge base load went off without problems.

        Returns:
            Tuple[int, int]: (node_count, link_count)
        """
        return self.query_engine.count_atoms()

    def query(
        self,
        query: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = {},
    ) -> Union[QueryAnswerIterator, List[Tuple[Assignment, Dict[str, str]]]]:
        """
        Perform a query on the knowledge base using a dict as input and return an
        iterator of QueryAnswer objects. Each such object carries the resulting mapping
        of variables in the query and the corresponding subgraph which is the result
        of ap[plying such mapping to rewrite the query.

        The input dict is a link, used as a pattern to make the query.
        Variables can be used as link targets as well as nodes. Nested links are
        allowed as well.

        Args:
            query (Dict[str, Any]): A pattern described as a link (possibly with nested links) with
            nodes and variables used to query the knoeledge base.
            paramaters (Dict[str, Any], optional): query optional parameters

        Returns:
            QueryAnswerIterator: An iterator of QueryAnswer objects, which have a field
            'assignment', with a mapping from variables to handles and another field
            'subgraph', with the resulting subgraph after applying 'assignment' to rewrite
            the query.

        Raises:
            UnexpectedQueryFormat: If query resolution lead to an invalid state

        Notes:
            - No logical connectors (AND, OR, NOT) are allowed
            - If no match is found for the query, an empty list is returned.

        Example:

            >>> das.add_link({
                "type": "Expression",
                "targets": [
                    {"type": "Symbol", "name": "Test"},
                    {
                        "type": "Expression",
                        "targets": [
                            {"type": "Symbol", "name": "Test"},
                            {"type": "Symbol", "name": "2"}
                        ]
                    }
                ]
            })
            >>> query_params = {"toplevel_only": False}
            >>> q1 = {
                "atom_type": "link",
                "type": "Expression",
                "targets": [
                    {"atom_type": "variable", "name": "v1"},
                    {
                        "atom_type": "link",
                        "type": "Expression",
                        "targets": [
                            {"atom_type": "variable", "name": "v2"},
                            {"atom_type": "node", "type": "Symbol", "name": "2"},
                        ]
                    }
                ]
            }
            >>> for result in das.query(q1, query_params):
            >>>     print(result.assignment.mapping['v1'])
            >>>     print(result.assignment.mapping['v2'])
            >>>     print(result.assignment.subgraph)
            '233d9a6da7d49d4164d863569e9ab7b6'
            '963d66edfb77236054125e3eb866c8b5'
            [
                {
                    'handle': 'dbcf1c7b610a5adea335bf08f6509978',
                    'type': 'Expression',
                    'template': ['Expression', 'Symbol', ['Expression', 'Symbol', 'Symbol']],
                    'targets': [
                        {'handle': '963d66edfb77236054125e3eb866c8b5', 'type': 'Symbol', 'name': 'Test'},
                        {
                            'handle': '233d9a6da7d49d4164d863569e9ab7b6',
                            'type': 'Expression',
                            'template': ['Expression', 'Symbol', 'Symbol'],
                            'targets': [
                                {'handle': '963d66edfb77236054125e3eb866c8b5', 'type': 'Symbol', 'name': 'Test'},
                                {'handle': '9f27a331633c8bc3c49435ffabb9110e', 'type': 'Symbol', 'name': '2'}
                            ]
                        }
                    ]
                }
            ]
        """
        return self.query_engine.query(query, parameters)

    def commit_changes(self):
        """This method applies changes made locally to the remote server"""
        self.query_engine.commit()

    @staticmethod
    def get_node_handle(node_type: str, node_name: str) -> str:
        """
        This method retrieves a handle from the node parameters

        Args:
            node_type (str): The type of the node being queried.
            node_name (str): The name of the specific node being queried.

        Returns:
            str: A handle

        Example:
            >>> result = das.get_node_handle(node_type='Concept', node_name='human')
            >>> print(result)
            "af12f10f9ae2002a1607ba0b47ba8407"
        """
        return AtomDB.node_handle(node_type, node_name)

    @staticmethod
    def get_link_handle(link_type: str, link_targets: List[str]) -> str:
        """
        This method retrieves a handle from the link parameters.

        Args:
            link_type (str): The type of the link being queried.
            link_targets (List[str]): A list of target identifiers that the link is associated with.

        Returns:
           str: A handle

        Example:
            >>> result = das.get_link(link_type='Similarity', targets=['af12f10f9ae2002a1607ba0b47ba8407', '1cdffc6b0b89ff41d68bec237481d1e1'])
            >>> print(result)
            "bad7472f41a0e7d601ca294eb4607c3a"

        """
        return AtomDB.link_handle(link_type, link_targets)

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a node to the database.

        This method allows you to add a node to the database
        with the specified node parameters. A node must have 'type' and
        'name' fields in the node_params dictionary.

        Args:
            node_params (Dict[str, Any]): A dictionary containing node parameters. It should have the following keys:
                - 'type': The type of the node.
                - 'name': The name of the node.

        Returns:
            Dict[str, Any]: The information about the added node, including its unique key and other details.

        Raises:
            AddNodeException: If the 'type' or 'name' fields are missing in node_params.

        Example:
            >>> node_params = {
                    'type': 'Reactome',
                    'name': 'Reactome:R-HSA-164843',
                }
            >>> das.add_node(node_params)
        """
        return self.backend.add_node(node_params)

    def add_link(self, link_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a link to the database.

        This method allows to add a link to the database with the specified link parameters.
        A link must have a 'type' and 'targets' field in the link_params dictionary.

        Args:
            link_params (Dict[str, Any]): A dictionary containing link parameters. It should have the following keys:
                - 'type': The type of the link.
                - 'targets': A list of target elements.

        Returns:
            Dict[str, Any]: The information about the added link, including its unique key and other details.

        Raises:
            AddLinkException: If the 'type' or 'targets' fields are missing in link_params.

        Example:
            >>> link_params = {
                    'type': 'Evaluation',
                    'targets': [
                        {'type': 'Predicate', 'name': 'Predicate:has_name'},
                        {
                            'type': 'Set',
                            'targets': [
                                {'type': 'Reactome', 'name': 'Reactome:R-HSA-164843'},
                                {'type': 'Concept', 'name': 'Concept:2-LTR circle formation'},
                            ],
                        },
                    ],
                }
            >>> das.add_link(link_params)
        """
        return self.backend.add_link(link_params)

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]] = None):
        """
        Rebuild all indexes according to the passed specification

        Args:
            pattern_index_templates: indexes are specified by atom type in a dict mapping from atom types
                to a pattern template:

                {
                    <atom type>: <pattern template>
                }

                Pattern template is also a dict:

                {
                    "named_type": True/False
                    "selected_positions": [n1, n2, ...]
                }

                Pattern templates are applied to each link entered in the atom space in order to determine
                which entries should be created in the inverted pattern index. Entries in the inverted
                pattern index are like patterns where the link type and each of its targets may be replaced
                by wildcards. For instance, given a similarity link Similarity(handle1, handle2) it could be
                used to create any of the following entries in the inverted pattern index:

                    *(handle1, handle2)
                    Similarity(*, handle2)
                    Similarity(handle1, *)
                    Similarity(*, *)

                If we create all possibilities of index entries to all links, the pattern index size will
                grow exponentially so we limit the entries we want to create by each type of link. This is
                what a pattern template for a given link type is. For instance if we apply this pattern
                template:

                {
                    "named_type": False
                    "selected_positions": [0, 1]
                }

                to Similarity(handle1, handle2) we'll create only the following entries:

                    Similarity(*, handle2)
                    Similarity(handle1, *)
                    Similarity(*, *)

                If we apply this pattern template instead:

                {
                    "named_type": True
                    "selected_positions": [1]
                }

                We'll have:

                    *(handle1, handle2)
                    Similarity(handle1, *)
        """
        return self.query_engine.reindex(pattern_index_templates)

    def clear(self) -> None:
        """Clear all data"""
        self.backend.clear_database()
        logger().debug('The database has been cleaned.')

    def get_traversal_cursor(self, handle: str, **kwargs) -> TraverseEngine:
        """Create an instance of the TraverseEngine

        Args:
            handle (str): atom handle

        Raises:
            GetTraversalCursorException: If Atom does not exist

        Returns:
            TraverseEngine: The object that allows traversal of the hypergraph
        """
        try:
            self.get_atom(handle)
        except AtomDoesNotExist:
            raise GetTraversalCursorException(message="Cannot start Traversal. Atom does not exist")

        return TraverseEngine(handle, das=self, **kwargs)
