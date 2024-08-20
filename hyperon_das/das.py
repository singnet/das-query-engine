from typing import Any, Dict, Iterator, List, Optional, Type, Union

from hyperon_das_atomdb import AtomDB, AtomDoesNotExist
from hyperon_das_atomdb.adapters import InMemoryDB, RedisMongoDB
from hyperon_das_atomdb.database import IncomingLinksT
from hyperon_das_atomdb.exceptions import InvalidAtomDB

from hyperon_das.cache.cache_controller import CacheController
from hyperon_das.constants import DasType
from hyperon_das.context import Context
from hyperon_das.exceptions import (
    GetTraversalCursorException,
    InvalidDASParameters,
    InvalidQueryEngine,
)
from hyperon_das.logger import logger
from hyperon_das.query_engines.local_query_engine import LocalQueryEngine
from hyperon_das.query_engines.remote_query_engine import RemoteQueryEngine
from hyperon_das.traverse_engines import TraverseEngine
from hyperon_das.type_alias import Query
from hyperon_das.utils import QueryAnswer, get_package_version


class DistributedAtomSpace:
    backend: AtomDB

    def __init__(self, system_parameters: Dict[str, Any] = {}, **kwargs) -> None:
        """
        Creates a new DAS object.
        A DAS client can run locally or locally and remote, connecting to remote DASs instances to query remote atoms,
        if there're different versions of the same atom in local and one of the remote DASs, the local version is returned.
        When running along a remote DAS a host and port is mandatory, by default local instances of the DBs are created,
        remote instances can be configured using kwargs options.


        Args:
            system_parameters (Dict[str, Any]): Sets the system parameters. Defaults to {
                'running_on_server': False, 'cache_enabled': False, 'attention_broker_hostname': 'localhost',
                'attention_broker_port': 27000}.

        Keyword Args:
            atomdb (str, optional): AtomDB type supported values are 'ram' and 'redis_mongo'. Defaults to 'ram'.
            query_engine (str, optional): Set the type of connection for the query engine, values are 'remote' or
                'local'.
                When this arg is set to 'remote', additional kwargs are required as host and port to connect
                to the remote query engine
                and the arg mode is used to configure the read/write privileges.
                Defaults to 'local'
            host (str, optional): Sets the host for the remote query engine, it's mandatory
                when the query_engine is equal to 'remote'.
            port (str, optional): Sets the port for the remote query engine, it's mandatory
                when the query_engine is equal to 'remote'.
            mode (str, optional): Set query engine's ACL privileges, only available
                when the query_engine is set to 'remote', accepts 'read-only' or 'read-write'.
                Defaults to 'read-only'
            mongo_hostname (str, optional): MongoDB's hostname, the local or remote query engine can
                connect to a remote server or run locally.
                Defaults to 'localhost'
            mongo_port (int, optional): MongoDB port, set this arg if the port is not the standard. Defaults to 27017.
            mongo_username (str, optional): Username used for authentication in the MongoDB database.
                Defaults to 'mongo'.
            mongo_password (str, optional): Password used for authentication in the MongoDB database.
                Defaults to 'mongo'.
            mongo_tls_ca_file (Any, optional): Full system path to the TLS certificate.
            redis_hostname (str, optional): Redis hostname, the local or remote query engine can connect
                to a remote server or run locally. Defaults to 'localhost'
            redis_port (int, optional): Redis port, set this arg if the port is not the standard. Defaults to 6379.
            redis_username (str, optional): Username used for authentication in the Redis database,
                no credentials (username/password) are needed when running locally.
            redis_password (str, optional): Password used for authentication in the Redis database.
            redis_cluster (bool, optional): Indicates whether Redis is configured in cluster mode. Defaults to True.
            redis_ssl (bool, optional): Set Redis to encrypt the connection. Defaults to True.
        """
        self.system_parameters = system_parameters
        self.atomdb = kwargs.get('atomdb', 'ram')
        self.query_engine_type = kwargs.get('query_engine', 'local')
        self._set_default_system_parameters()
        self._set_backend(**kwargs)
        self.cache_controller = CacheController(self.system_parameters)
        self._set_query_engine(**kwargs)

    def _set_default_system_parameters(self) -> None:
        # Internals
        if not self.system_parameters.get('running_on_server'):
            self.system_parameters['running_on_server'] = False
        # Attention Broker
        if not self.system_parameters.get('cache_enabled'):
            self.system_parameters['cache_enabled'] = False
        if not self.system_parameters.get('attention_broker_hostname'):
            self.system_parameters['attention_broker_hostname'] = 'localhost'
        if not self.system_parameters.get('attention_broker_port'):
            self.system_parameters['attention_broker_port'] = 27000

    def _set_backend(self, **kwargs) -> None:
        if self.atomdb == "ram":
            self.backend = InMemoryDB()
        elif self.atomdb == "redis_mongo":
            self.backend = RedisMongoDB(**kwargs)
            if self.query_engine_type != "local":
                raise InvalidDASParameters(
                    message="'redis_mongo' AtomDB requires local query engine (i.e. 'query_engine=local')"
                )
        else:
            raise InvalidAtomDB(message="Invalid AtomDB type. Choose either 'ram' or 'redis_mongo'")

    def _set_query_engine(self, **kwargs) -> None:
        if self.query_engine_type == 'local':
            das_type = DasType.LOCAL_RAM_ONLY if self.atomdb == "ram" else DasType.LOCAL_REDIS_MONGO
            self._start_query_engine(LocalQueryEngine, das_type, **kwargs)
        elif self.query_engine_type == "remote":
            self._start_query_engine(RemoteQueryEngine, DasType.REMOTE, **kwargs)
        else:
            raise InvalidQueryEngine(
                message="Use either 'local' or 'remote'",
                details=f"query_engine={self.query_engine_type}",
            )

    def _start_query_engine(
        self,
        engine_type: Type[LocalQueryEngine | RemoteQueryEngine],
        das_type: str,
        **kwargs,
    ) -> None:
        self._das_type = das_type
        self.query_engine = engine_type(
            self.backend, self.cache_controller, self.system_parameters, **kwargs
        )
        logger().info(f"Started {das_type} DAS")

    def _create_context(
        self,
        name: str,
        queries: List[Query] = [],
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
            >>> das = DistributedAtomSpace()
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
            >>> das = DistributedAtomSpace()
            >>> human_handle = das.get_node_handle(node_type='Concept', node_name='human')
            >>> monkey_handle = das.get_node_handle(node_type='Concept', node_name='monkey')
            >>> result = das.get_link_handle(link_type='Similarity', targets=[human_handle, monkey_handle])
            >>> print(result)
            "bad7472f41a0e7d601ca294eb4607c3a"

        """
        return AtomDB.link_handle(link_type, link_targets)

    def get_atom(self, handle: str, **kwargs) -> Dict[str, Any]:
        """
        Retrieve an atom given its handle, handles for atoms can be created by using the function 'get_node_handle'.
        A handle is MD5 hash of a node in the graph.


        Args:
            handle (str): Atom's handle.

        Keyword Args:
            no_target_format (bool, optional): Set this parameter to True to get MongoDB's default output. Defaults to False.
            targets_document (bool, optional): Set this parameter to True to return a tuple containing the document as first element
                and the targets as second element. Defaults to False.

        Returns:
            Dict: A Python dict with all atom data.

        Raises:
            AtomDoesNotExist: If the corresponding atom doesn't exist.

        Examples:
            >>> das = DistributedAtomSpace()
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

        Args:self.query_engine
            node_type (str): Node type
            node_name (str): Node name

        Returns:
            Dict: A Python dict with all node data.

        Raises:
            AtomDoesNotExist: If the corresponding node doesn't exist.

        Examples:
            >>> das = DistributedAtomSpace()
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
        node_handle = self.backend.node_handle(node_type, node_name)
        return self.get_atom(node_handle)

    def get_link(self, link_type: str, link_targets: List[str]) -> Dict[str, Any]:
        """
        Retrieve a link given its type and list of targets.
        Targets are hashes of the nodes these hashes or handles can be created using the function 'get_node_handle'.

        Args:
            link_type (str): Link type
            link_targets (List[str]): List of target handles.

        Returns:
            Dict: A Python dict with all link data.

        Raises:
            AtomDoesNotExist: If the corresponding link doesn't exist.

        Examples:
            >>> das = DistributedAtomSpace()
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
        link_handle = self.backend.link_handle(link_type, link_targets)
        return self.get_atom(link_handle)

    def get_links(
        self,
        link_type: str,
        target_types: list[str] | None = None,
        link_targets: list[str] | None = None,
        **kwargs,
    ) -> Union[Iterator, List[str], List[Dict], tuple[int, List[Dict]]]:  # TODO: simplify
        """
        Retrieve all links that match the passed search criteria.

        This method can be used in four different ways.

        1. Retrieve all the links of a given type

            Set link_type to the desired type and set target_types=None and
            link_targets=None.

        2. Retrieve all the links of a given type whose targets are of given types.

            Set link_type to the desired type and target_types to a list with the desired
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

        Keyword Args:
            no_iterator (bool, optional): Set False to return an iterator otherwise it will return a
                list of Dict[str, Any].
                If the query_engine is set to 'local' it always return an iterator.
                Defaults to True.
            cursor (int, optional): Cursor position in the iterator, starts retrieving links from redis at the cursor
                position. Defaults to 0.
            chunk_size (int, optional): Chunk size. Defaults to 1000.
            top_level_only (bool optional): Set to True to filter top level links. Defaults to False.


        Returns:
            Union[Iterator, List[Dict[str, Any]]]: A list of dictionaries containing detailed
            information of the links

        Examples:

            1. Retrieve all the links of a given type
                >>> das = DistributedAtomSpace()
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

    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> tuple[int | None, IncomingLinksT | Iterator]:
        """
        Retrieve all links which has the passed handle as one of its targets.

        Args:
            atom_handle (str): Atom's handle

        Keyword Args:
            no_iterator (bool, optional): Set False to return an iterator otherwise it will return a list of Dict[str, Any].
                If the query_engine is set to 'remote' it always return an iterator.
                Defaults to True.
            cursor (int, optional): Cursor position in the iterator, starts retrieving links from redis at the cursor position. Defaults to 0.
            handles_only (bool, optional): Returns a list of links handles.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing detailed information of the atoms
            or a list of strings containing the atom handles

        Examples:
            >>> das = DistributedAtomSpace()
            >>> rhino = das.get_node_handle('Concept', 'rhino')
            >>> links = das.get_incoming_links(rhino)
            >>> for link in links:
            >>>     print(link['type'], link['targets'])
            Similarity ['d03e59654221c1e8fcda404fd5c8d6cb', '99d18c702e813b07260baf577c60c455']
            Similarity ['99d18c702e813b07260baf577c60c455', 'd03e59654221c1e8fcda404fd5c8d6cb']
            Inheritance ['99d18c702e813b07260baf577c60c455', 'bdfe4e7a431f73386f37c6448afe5840']
        """
        return self.query_engine.get_incoming_links(atom_handle, **kwargs)

    def count_atoms(self, parameters: Dict[str, Any] = []) -> Dict[str, int]:
        """
        Count atoms, nodes and links in DAS.

        By default, the precise parameter is set to False returning the total number of atoms, without node and link
        counts. If the precise parameter is True it will return the total of nodes and links and atoms.

        In the case of remote DAS, count the total number of nodes and links stored locally and
        remotely. If there are more than one instance of the same atom (local and remote), it's
        counted only once.

        Args:
            parameters (Optional[Dict[str, Any]]): Dict containing the following keys: 'context' - returning the
                count of 'local', 'remote' or 'both', 'precise' - boolean if True provides an accurate but slower count,
                if False the count will be an estimate, which is faster but less precise.
                Default value for 'context' is 'both' and for 'precise' is False.
                Defaults to None.

        Returns:
            Dict[str, int]: Dict containing the keys 'node_count', 'atom_count', 'link_count'.
        """
        return self.query_engine.count_atoms(parameters)

    def query(
        self,
        query: Query,
        parameters: Dict[str, Any] = {},
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
            parameters (Dict[str, Any]): query optional parameters, defaults to {}

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

    def custom_query(
        self, index_id: str, query: Query, **kwargs
    ) -> Union[Iterator, List[Dict[str, Any]]]:
        """
        Perform a query using a previously created custom index.

        Actual query parameters can be passed as kwargs according to the type of the previously
        created filter.

        Args:
            index_id (str): custom index id to be used in the query.
            query (Dict[str, Any]): Query dict, fields are the dict's keys and values are the search.
                It supports multiple fields.
                eg: {'name': 'human'}

        Keyword Args:
            no_iterator (bool, optional): Set False to return an iterator otherwise it will
                return a list of Dict[str, Any].
                If the query_engine is set to 'remote' it always return an iterator.
                Defaults to True.
            cursor (Any, optional): Cursor position in the iterator, starts retrieving links from redis at the cursor
                position. Defaults to 0.
            chunk_size (int, optional): Chunk size. Defaults to 1000.

        Raises:
            NotImplementedError: If called from Local DAS in RAM only.

        Returns:
            Union[Iterator, List[Dict[str, Any]]]: An iterator or list of dict containing atom data.

        Examples:
            >>> das.custom_query(index_id='index_123', query={'tag': 'DAS'})
            >>> das.custom_query(index_id='index_123', query={'tag': 'DAS'}, no_iterator=True)
        """
        if isinstance(self.query_engine, LocalQueryEngine) and isinstance(self.backend, InMemoryDB):
            raise NotImplementedError("custom_query() is not implemented for Local DAS in RAM only")

        return self.query_engine.custom_query(
            index_id, [{'field': k, 'value': v} for k, v in query.items()], **kwargs
        )

    def get_atoms_by_field(self, query: Query) -> List[str]:
        """
        Search for the atoms containing field and value, performance is improved if an index was
        previously created.

        Args:
            query (Dict[str, Any]): Query dict, fields are the dict keys and values are the search.
                It supports multiple fields.
                eg: {'name': 'human'}

        Returns:
            List[str]: List of atom's ids
        """

        return self.query_engine.get_atoms_by_field(
            [{'field': k, 'value': v} for k, v in query.items()]
        )

    def get_atoms_by_text_field(
        self, text_value: str, field: Optional[str] = None, text_index_id: Optional[str] = None
    ) -> List[str]:
        """
        Performs a text search, if a text index is previously created performance a token index search,
        otherwise will perform a regex search using binary tree and the argument 'field' is mandatory.
        Performance is improved if a 'binary_tree' or 'token_inverted_list' is previously created using
        'create_field_index' method.

        Args:
            text_value (str): Text value to search for
            field (Optional[str]): Field to check the text_value
            text_index_id (Optional[str]): Text index
        Returns:
            List[str]: List of atom's ids
        """
        return self.query_engine.get_atoms_by_text_field(text_value, field, text_index_id)

    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> List[str]:
        """
        Performs a search in the nodes names searchin for a node starting with the 'startswith'
        value.

        Args:
            node_type (str): Node type
            startswith (str): String to search for
        Returns:
            List[str]: List of atom's ids
        """
        return self.query_engine.get_node_by_name_starting_with(node_type, startswith)

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
            >>> das = DistributedAtomSpace()
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
        "type" should map to a string and "targets" to a list of Python dict, each of them being
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
            >>> das = DistributedAtomSpace()
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

        A TraverseEngine is like a cursor which points to an atom in the hypergraph and
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
        fields: List[str],
        named_type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
        index_type: Optional[str] = None,
    ) -> str:
        """
        Create a custom index on the passed field of all atoms of the passed type.

        Remote DAS allow creation of custom indexes based on custom fields in
        nodes or links. These indexes can be used to make subsequent custom queries.

        Args:
            atom_type (str): Either 'link' or 'node', if the index is to be created for
                links or nodes.
            fields (List[str]): fields where the index will be created upon
            named_type (str, optional): Only atoms of the passed type will be indexed. Defaults
                to None, meaning that atom type doesn't matter.
            composite_type (List[Any], optional): Only Atoms type of the passed composite
                type will be indexed. Defaults to None.
            index_type (Optional[str]): Type of index, values allowed are 'binary_tree' to create indexes using binary
                tree in ascending order or 'token_inverted_list' to create index for text field search the text field
                will be tokenized and every token will be an indexed. Only one token_inverted_list field is allowed.
                If set as None will create a binary tree index.
                Defaults to None.

        Raises:
            ValueError: If parameters are invalid somehow.

        Returns:
            str: The index ID. This ID should be used to make subsequent queries using this
                newly created index.

        Examples:
            >>> index_id = das.create_field_index('link', ['tag'], type='Expression')
            >>> index_id = das.create_field_index('link', ['tag'], composite_type=['Expression', 'Symbol', 'Symbol', ['Expression', 'Symbol', 'Symbol', 'Symbol']])
        """
        if named_type and composite_type:
            raise ValueError("'type' and 'composite_type' can't be specified simultaneously")

        if named_type and not isinstance(named_type, str):
            raise ValueError("'atom_type' should be str")

        if not fields:
            raise ValueError("'fields' should not be None or empty")

        return self.query_engine.create_field_index(
            atom_type,
            fields,
            named_type=named_type,
            composite_type=composite_type,
            index_type=index_type,
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
            if self._das_type != DasType.REMOTE and (not host or not port):
                raise ValueError("'host' and 'port' are mandatory parameters to local DAS")

        documents = self.query_engine.fetch(query, host, port, **kwargs)
        self.backend.bulk_insert(documents)
        return documents

    def create_context(
        self,
        name: str,
        queries: List[Query] = [],
    ) -> Context:
        if self.query_engine_type == 'local':
            return self._create_context(name, queries)
        else:
            return self.query_engine.create_context(name, queries)
