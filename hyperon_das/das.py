from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from hyperon_das_atomdb import AtomDB, AtomDoesNotExist
from hyperon_das_atomdb.adapters import InMemoryDB, RedisMongoDB
from hyperon_das_atomdb.exceptions import InvalidAtomDB

from hyperon_das.cache.cache_controller import CacheController
from hyperon_das.context import Context
from hyperon_das.exceptions import (
    GetTraversalCursorException,
    InvalidDASParameters,
    InvalidQueryEngine,
)
from hyperon_das.logger import logger
from hyperon_das.query_engines import LocalQueryEngine, RemoteQueryEngine
from hyperon_das.traverse_engines import TraverseEngine
from hyperon_das.type_alias import Query
from hyperon_das.utils import QueryAnswer, get_package_version


class DistributedAtomSpace:
    def __init__(self, system_parameters: Dict[str, Any] = {}, **kwargs) -> None:
        self.system_parameters = system_parameters
        self._set_default_system_parameters()
        atomdb = kwargs.get('atomdb', 'ram')
        query_engine = kwargs.get('query_engine', 'local')

        if atomdb == "ram":
            self.backend = InMemoryDB()
        elif atomdb == "redis_mongo":
            self.backend = RedisMongoDB(**kwargs)
            if query_engine != "local":
                raise InvalidDASParameters(
                    message="'redis_mongo' AtomDB requires local query engine (i.e. 'query_engine=local')"
                )
        else:
            raise InvalidAtomDB(message="Invalid AtomDB type. Choose either 'ram' or 'redis_mongo'")

        if query_engine == 'local':
            self._das_type = 'local_ram_only' if atomdb == 'ram' else 'local_redis_mongo'
            self.query_engine = LocalQueryEngine(self.backend, self.system_parameters, kwargs)
            logger().info('Started local DAS')
        elif query_engine == 'remote':
            self._das_type = 'remote'
            self.query_engine = RemoteQueryEngine(self.backend, self.system_parameters, kwargs)
            logger().info('Started remote DAS')
        else:
            raise InvalidQueryEngine(
                message="Use either 'local' or 'remote'",
                details=f'query_engine={query_engine}',
            )

        self.cache_controller = CacheController(self.system_parameters)

    def _set_default_system_parameters(self) -> None:
        # Internals
        if not self.system_parameters.get('running_on_server'):
            self.system_parameters['running_on_server'] = False
        # Attention Broker
        if not self.system_parameters.get('cache_enabled'):
            self.system_parameters['cache_enabled'] = False
        if not self.system_parameters.get('attention_broker_hostname'):
            self.system_parameters['attention_broker_hostname'] = "localhost"
        if not self.system_parameters.get('attention_broker_port'):
            self.system_parameters['attention_broker_port'] = 27000

    def _create_context(
        self,
        name: str,
        queries: Optional[List[Query]] = None,
    ) -> Context:
        context_node = self.add_node({'type': Context.CONTEXT_NODE_TYPE, 'name': name})
        query_answer = [self.query(query, {'no_iterator': True}) for query in queries]
        context = Context(context_node, query_answer)
        self.cache_controller.add_context(context)
        return context

    @staticmethod
    def about() -> dict:
        return {
            'das': {
                'name': 'hyperon-das',
                'version': get_package_version('hyperon_das'),
                'summary': 'Query Engine API for Distributed AtomSpace',
            },
            'atom_db': {
                'name': 'hyperon-das-atomdb',
                'version': get_package_version('hyperon_das_atomdb'),
                'summary': 'Persistence layer for Distributed AtomSpace',
            },
        }

    @staticmethod
    def get_node_handle(node_type: str, node_name: str) -> str:
        """
        Computes the handle of a node, given its type and name.

        Note that this is a static method which don't actually query the stored atomspace
        in order to compute the handle. Instead, it just run a MD5 hashing algorithm on
        the parameters that uniquely identify nodes (i.e. type and name) This means e.g.
        that two nodes with the same type and the same name are considered to be the exact
        same entity as they will have the same handle.

        Args:
            node_type (str): Node type
            node_name (str): Node name

        Returns:
            str: Node's handle

        Examples:
            >>> result = das.get_node_handle(node_type='Concept', node_name='human')
            >>> print(result)
            "af12f10f9ae2002a1607ba0b47ba8407"
        """
        return AtomDB.node_handle(node_type, node_name)

    @staticmethod
    def get_link_handle(link_type: str, link_targets: List[str]) -> str:
        """
        Computes the handle of a link, given its type and targets' handles.

        Note that this is a static method which don't actually query the stored atomspace
        in order to compute the handle. Instead, it just run a MD5 hashing algorithm on
        the parameters that uniquely identify links (i.e. type and list of targets) This
        means e.g. that two links with the same type and the same targets are considered
        to be the exact same entity as they will have the same handle.

        Args:
            link_type (str): Link type.
            link_targets (List[str]): List with the target handles.

        Returns:
           str: Link's handle.

        Examples:
            >>> human_handle = das.get_node_handle(node_type='Concept', node_name='human')
            >>> monkey_handle = das.get_node_handle(node_type='Concept', node_name='monkey')
            >>> result = das.get_link_handle(link_type='Similarity', targets=[human_handle, monkey_handle])
            >>> print(result)
            "bad7472f41a0e7d601ca294eb4607c3a"

        """
        return AtomDB.link_handle(link_type, link_targets)

    def get_atom(self, handle: str, **kwargs) -> Dict[str, Any]:
        """
        Retrieve an atom given its handle.

        Args:
            handle (str): Atom's handle.

        Returns:
            Dict: A Python dict with all atom data.

        Raises:
            AtomDoesNotExist: If the corresponding atom doesn't exist.

        Examples:
            >>> human_handle = das.get_node_handle(node_type='Concept', node_name='human')
            >>> result = das.get_atom(human_handle)
            >>> print(result)
            {
                'handle': 'af12f10f9ae2002a1607ba0b47ba8407',
                'composite_type_hash': 'd99a604c79ce3c2e76a2f43488d5d4c3',
                'name': 'human',
                'named_type': 'Concept'
            }
        """
        return self.query_engine.get_atom(handle, **kwargs)

    def get_node(self, node_type: str, node_name: str) -> Dict[str, Any]:
        """
        Retrieve a node given its type and name.

        Args:
            node_type (str): Node type
            node_name (str): Node name

        Returns:
            Dict: A Python dict with all node data.

        Raises:
            NodeDoesNotExist: If the corresponding node doesn't exist.

        Examples:
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

    def get_link(self, link_type: str, link_targets: List[str]) -> Dict[str, Any]:
        """
        Retrieve a link given its type and list of targets.

        Args:
            link_type (str): Link type
            link_targets (List[str]): List of target handles.

        Returns:
            Dict: A Python dict with all link data.

        Raises:
            LinkDoesNotExist: If the corresponding link doesn't exist.

        Examples:
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
        self,
        link_type: str,
        target_types: List[str] = None,
        link_targets: List[str] = None,
        **kwargs,
    ) -> Union[Iterator, List[Dict[str, Any]]]:
        """
        Retrieve all links that match the passed search criteria.

        This method can be used in four different ways.

        1. Retrieve all the links of a given type

            Set link_type to the desired type and set target_types=None and
            link_targets=None.

        2. Retrieve all the links of a given type whose targets are of given types.

            Set link_type to the disered type and target_types to a list with the desired
            types os each target.

        3. Retrieve all the links of a given type whose targets match a given list of
           handles

            Set link_type to the desired type (or pass link_type='*' to retrieve links
            of any type) and set link_targets to a list of handles. Any handle in this
            list can be '*' meaning that any handle in that position of the targets list
            is a match for the query. Set target_types=None.

        Args:
            link_type (str): Link type being searched (can be '*' when link_targets is not None).
            target_types (List[str], optional): Template of target types being searched.
            link_targets (List[str], optional): Template of targets being searched (handles or '*').

        Returns:
            Union[Iterator, List[Dict[str, Any]]]: A list of dictionaries containing detailed
            information of the links

        Examples:

            1. Retrieve all the links of a given type

                >>> links = das.get_links(link_type='Inheritance')
                >>> for link in links:
                >>>     print(link['type'], link['targets'])
                Inheritance ['5b34c54bee150c04f9fa584b899dc030', 'bdfe4e7a431f73386f37c6448afe5840']
                Inheritance ['b94941d8cd1c0ee4ad3dd3dcab52b964', '80aff30094874e75028033a38ce677bb']
                Inheritance ['bb34ce95f161a6b37ff54b3d4c817857', '0a32b476852eeb954979b87f5f6cb7af']
                ...

            2. Retrieve all the links of a given type whose targets are of given types.

                >>> links = das.get_links(link_type='Inheritance', target_types=['Concept', 'Concept'])
                >>> for link in links:
                >>>     print(link['type'], link['targets'])
                Inheritance ['5b34c54bee150c04f9fa584b899dc030', 'bdfe4e7a431f73386f37c6448afe5840']
                Inheritance ['b94941d8cd1c0ee4ad3dd3dcab52b964', '80aff30094874e75028033a38ce677bb']
                Inheritance ['bb34ce95f161a6b37ff54b3d4c817857', '0a32b476852eeb954979b87f5f6cb7af']
                ...

            3. Retrieve all the links of a given type whose targets match a given list of
               handles

                >>> snake = das.get_node_handle('Concept', 'snake')
                >>> links = das.get_links(link_type='Similarity', link_targets=[snake, '*'])
                >>> for link in links:
                >>>     print(link['type'], link['targets'])
                Similarity ['c1db9b517073e51eb7ef6fed608ec204', 'b94941d8cd1c0ee4ad3dd3dcab52b964']
                Similarity ['c1db9b517073e51eb7ef6fed608ec204', 'bb34ce95f161a6b37ff54b3d4c817857']
        """
        return self.query_engine.get_links(link_type, target_types, link_targets, **kwargs)

    def get_incoming_links(self, atom_handle: str, **kwargs) -> List[Union[Dict[str, Any], str]]:
        """
        Retrieve all links which has the passed handle as one of its targets.

        Args:
            atom_handle (str): Atom's handle

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing detailed information of the atoms
            or a list of strings containing the atom handles

        Examples:
            >>> rhino = das.get_node_handle('Concept', 'rhino')
            >>> links = das.get_incoming_links(rhino)
            >>> for link in links:
            >>>     print(link['type'], link['targets'])
            Similarity ['d03e59654221c1e8fcda404fd5c8d6cb', '99d18c702e813b07260baf577c60c455']
            Similarity ['99d18c702e813b07260baf577c60c455', 'd03e59654221c1e8fcda404fd5c8d6cb']
            Inheritance ['99d18c702e813b07260baf577c60c455', 'bdfe4e7a431f73386f37c6448afe5840']
        """
        return self.query_engine.get_incoming_links(atom_handle, **kwargs)

    def count_atoms(self) -> Tuple[int, int]:
        """
        Count nodes and links in DAS.

        In the case of remote DAS, count the total number of nodes and links stored locally and
        remotelly. If there are more than one instance of the same atom (local and remote), it's
        counted only once.

        Returns:
            Tuple[int, int]: (node_count, link_count)
        """
        return self.query_engine.count_atoms()

    def query(
        self,
        query: Query,
        parameters: Optional[Dict[str, Any]] = {},
    ) -> Union[Iterator[QueryAnswer], List[QueryAnswer]]:
        """
        Perform a query on the knowledge base using a dict as input and return an
        iterator of QueryAnswer objects. Each such object carries the resulting mapping
        of variables in the query and the corresponding subgraph which is the result
        of applying such mapping to rewrite the query.

        The input dict is a link, used as a pattern to make the query.
        Variables can be used as link targets as well as nodes. Nested links are
        allowed as well.

        Args:
            query (Union[List[Dict[str, Any]], Dict[str, Any]]): A pattern described as a
                link (possibly with nested links) with nodes and variables used to query
                the knowledge base. If the query is represented as a list of dictionaries,
                it is interpreted as a conjunction (AND) of all queries within the list.
            parameters (Dict[str, Any], optional): query optional parameters

        Returns:
            Iterator[QueryAnswer]: An iterator of QueryAnswer objects, which have a field 'assignment',
                with a mapping from variables to handles and another field 'subgraph',
                with the resulting subgraph after applying 'assignment' to rewrite the query.

        Raises:
            UnexpectedQueryFormat: If query resolution lead to an invalid state

        Notes:
            - Logical connectors OR and NOT are not implemented yet.
            - If no match is found for the query, an empty list is returned.

        Examples:

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

    def custom_query(self, index_id: str, **kwargs) -> Union[Iterator, List[Dict[str, Any]]]:
        """
        Perform a query using a previously created custom index.

        Actual query parameters can be passed as kwargs according to the type of the previously
        created filter.

        Args:
            index_id (str): custom index id to be used in the query.

        Raises:
            NotImplementedError: If called from Local DAS in RAM only.

        Returns:
            Union[Iterator, List[Dict[str, Any]]]: An iterator or list of dict containing atom data.

        Examples:
            >>> das.custom_query(index_id='index_123', tag='DAS')
            >>> das.custom_query(index_id='index_123', tag='DAS', no_iterator=True)
        """
        if isinstance(self.query_engine, LocalQueryEngine) and isinstance(self.backend, InMemoryDB):
            raise NotImplementedError("custom_query() is not implemented for Local DAS in RAM only")

        return self.query_engine.custom_query(index_id, **kwargs)

    def commit_changes(self, **kwargs):
        """
        Commit changes (atom addition/deletion/change) to the databases or to
        the remote DAS Server, depending on the type of DAS being used.

        The behavior of this method depends on the type of DAS being used.

        1. When called in a DAS instantiated with query_engine=remote

            This is called a "Remote DAS" in the documentation. Remote DAS is
            connected to a remote DAS Server which is used to make queries,
            traversing, etc but it also keeps a local Atomspace in RAM which is
            used as a cache. Atom changes are made initially in this local cache.
            When commit_changes() is called in this type of DAS, these changes are
            propagated to the remote DAS Server.

        2. When called in a DAS instantiated with query_engine=local and
           atomdb='ram'.

            No effect.

        3. When called in a DAS instantiated with query_engine=local and
           atomdb='redis_mongo'

            The AtomDB keeps buffers of changes which are not actually written in the
            DBs until commit_changes() is called (or until that buffers size reach a
            threshold).
        """
        self.query_engine.commit(**kwargs)

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a node to DAS.

        A node is represented by a Python dict which may contain any number of keys associated to
        values of any type (including lists, sets, nested dicts, etc) , which are all
        recorded with the node, but must contain at least the keys "type" and "name"
        mapping to strings which define the node uniquely, i.e. two nodes with the same
        "type" and "name" are considered to be the same entity.

        Args:
            node_params (Dict[str, Any]): A dictionary with node data. The following keys are mandatory:
                - 'type': Node type
                - 'name': Node name

        Returns:
            Dict[str, Any]: The information about the added node, including its unique handle and
            other fields used internally in DAS.

        Raises:
            AddNodeException: If 'type' or 'name' fields are missing or invalid somehow.

        Examples:
            >>> node_params = {
                    'type': 'Reactome',
                    'name': 'Reactome:R-HSA-164843',
                }
            >>> das.add_node(node_params)
        """
        return self.backend.add_node(node_params)

    def add_link(self, link_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds a link to DAS.

        A link is represented by a Python dict which may contain any number of keys associated to
        values of any type (including lists, sets, nested dicts, etc) , which are all
        recorded with the link, but must contain at least the keys "type" and "targets".
        "type" shpould map to a string and "targets" to a list of Python dict, each of them being
        itself a representation of either a node or a nested link. "type" and "targets" define the
        link uniquely, i.e. two links with the same "type" and "targets" are considered to be the
        same entity.

        Args:
            link_params (Dict[str, Any]): A dictionary with link data. The following keys are mandatory:
                - 'type': The type of the link.
                - 'targets': A list of target elements.

        Returns:
            Dict[str, Any]: The information about the added link, including its unique handle and
            other fields used internally in DAS.

        Raises:
            AddLinkException: If the 'type' or 'targets' fields are missing or invalid somehow.

        Examples:
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
        """
        Delete all atoms and custom indexes.
        """
        self.backend.clear_database()
        logger().debug('The database has been cleaned.')

    def get_traversal_cursor(self, handle: str, **kwargs) -> TraverseEngine:
        """
        Create and return a [Traverse Engine](/api/Traverse Engine), an object that can be used to traverse the
        atomspace hypergraph.

        A TraverseEngine is like a cusor which points to an atom in the hypergraph and
        can be used to probe for links and neighboring atoms and then move on by
        following links. It's functioning is closely tied to the cache system in order
        to optimize the order in which atoms are presented to the caller when probing
        the neighborhood and to use cache's "atom paging" capabilities to minimize
        latency when used in remote DAS.

        Args:
            handle (str): Atom's handle

        Raises:
            GetTraversalCursorException: If passed handle is invalid, somehow (e.g. if
            the corresponding atom doesn't exist).

        Returns:
            TraverseEngine: The object that allows traversal of the hypergraph.
        """
        try:
            return TraverseEngine(handle, das=self, **kwargs)
        except AtomDoesNotExist:
            raise GetTraversalCursorException(message="Cannot start Traversal. Atom does not exist")

    def create_field_index(
        self,
        atom_type: str,
        field: str,
        type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
    ) -> str:
        """
        Create a custom index on the passed field of all atoms of the passed type.

        Remote DAS allow creation of custom indexes based on custom fields in
        nodes or links. These indexes can be used to make subsequent custom queries.

        Args:
            atom_type (str): Either 'link' or 'node', if the index is to be created for
                links or nodes.
            field (str): field where the index will be created upon
            type (str, optional): Only atoms of the passed type will be indexed. Defaults
                to None, meaning that atom type doesn't matter.
            composite_type (List[Any], optional): Only Atoms type of the passed composite
                type will be indexed. Defaults to None.

        Raises:
            ValueError: If parameters are invalid somehow.

        Returns:
            str: The index ID. This ID should be used to make subsequent queries using this
                newly created index.

        Examples:
            >>> index_id = das.create_field_index('link', 'tag', type='Expression')
            >>> index_id = das.create_field_index('link', 'tag', composite_type=['Expression', 'Symbol', 'Symbol', ['Expression', 'Symbol', 'Symbol', 'Symbol']])
        """
        if type and composite_type:
            raise ValueError("'type' and 'composite_type' can't be specified simultaneously")

        if type and not isinstance(type, str):
            raise ValueError("'atom_type' should be str")

        return self.query_engine.create_field_index(
            atom_type, field, type=type, composite_type=composite_type
        )

    def fetch(
        self,
        query: Optional[Union[List[dict], dict]] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs,
    ) -> Union[None, List[dict]]:
        """
        Fetch, from a DAS Server, all atoms that match the passed query or
        all atoms in the server if None is passed as query.

        Instead of adding atoms by calling add_node() and add_link() directly,
        it's possible to fetch all or part of the contents from a DAS server using the
        method fetch(). This method doesn't create a lasting connection with the DAS
        server, it will just fetch the atoms once and close the connection so any
        subsequent changes or queries will not be propagated to the server in any way.
        After fetching the atoms, all queries will be made locally. It's possible to
        call fetch() multiple times fetching from the same DAS Server or from different
        ones.

        The input query is a link, used as a pattern to make the query.
        Variables can be used as link targets as well as nodes. Nested links are
        allowed as well.

        Args:
            query (Optional[Union[List[dict], dict]]): A pattern described as a link (possibly with nested links)
                with nodes and variables used to query the knowledge base. Defaults to None
            host (Optional[str], optional): Address to remote server. Defaults to None.
            port (Optional[int], optional): Port to remote server. Defaults to None.

        Raises:
            ValueError: If parameters ar somehow invalid.

        Returns:
            Union[None, List[dict]]: Returns None.
            If runing on the server returns a list of dictionaries containing detailed information of the atoms.

        Examples:
            >>> query = {
                    "atom_type": "link",
                    "type": "Expression",
                    "targets": [
                        {"atom_type": "node", "type": "Symbol", "name": "Inheritance"},
                        {"atom_type": "variable", "name": "v1"},
                        {"atom_type": "node", "type": "Symbol", "name": '"mammal"'},
                    ],
                }
                das = DistributedAtomSpace()
                das.fetch(query, host='123.4.5.6', port=8080)
        """

        if not self.system_parameters.get('running_on_server'):
            if self._das_type != 'remote' and (not host or not port):
                raise ValueError("'host' and 'port' are mandatory parameters to local DAS")

        documents = self.query_engine.fetch(query, host, port, **kwargs)
        self.backend.bulk_insert(documents)
        return documents

    def create_context(
        self,
        name: str,
        queries: Optional[List[Query]] = None,
    ) -> Context:
        if self.system_parameters.get('running_on_server'):
            return self._create_context(name, queries)
        else:
            return self.query_engine.create_context(name, queries)
