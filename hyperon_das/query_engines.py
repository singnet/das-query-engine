import re
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

from hyperon_das_atomdb import WILDCARD, AtomDB
from hyperon_das_atomdb.adapters import InMemoryDB
from hyperon_das_atomdb.exceptions import AtomDoesNotExist, LinkDoesNotExist, NodeDoesNotExist

from hyperon_das.cache.iterators import (
    AndEvaluator,
    CustomQuery,
    LazyQueryEvaluator,
    ListIterator,
    LocalGetLinks,
    LocalIncomingLinks,
    QueryAnswerIterator,
    RemoteGetLinks,
    RemoteIncomingLinks,
)
from hyperon_das.client import FunctionsClient
from hyperon_das.context import Context
from hyperon_das.exceptions import (
    InvalidDASParameters,
    QueryParametersException,
    UnexpectedQueryFormat,
)
from hyperon_das.logger import logger
from hyperon_das.type_alias import Query
from hyperon_das.utils import Assignment, QueryAnswer, das_error


class QueryEngine(ABC):
    @abstractmethod
    def get_atom(self, handle: str) -> Union[Dict[str, Any], None]:
        """
        Retrieves an atom from the database using its unique handle.

        This method searches the database for an atom with the specified handle. If found, it returns
        the atom's data as a dictionary. If no atom with the given handle exists, it returns None.

        Args:
            handle (str): The unique handle of the atom to retrieve.

        Returns:
            Union[Dict[str, Any], None]: A dictionary containing the atom's data if found, otherwise None.
        """
        ...

    @abstractmethod
    def get_node(self, node_type: str, node_name: str) -> Union[Dict[str, Any], None]:
        """
        Retrieves a node of a specified type and name.

        This method searches for a node that matches the specified type and name. If such a node exists,
        it returns the node's data; otherwise, it returns None.

        Args:
            node_type (str): The type of the node to retrieve.
            node_name (str): The name of the node to retrieve.

        Returns:
            Union[Dict[str, Any], None]: A dictionary containing the node's data if found, otherwise None.
        """
        ...

    @abstractmethod
    def get_link(self, link_type: str, targets: List[str]) -> Union[Dict[str, Any], None]:
        """
        Retrieves a link of a specified type that connects to the given targets.

        This method searches for a link that matches the specified link type and is connected to
        the provided target nodes. The targets are specified by their handles. If such a link exists,
        it returns the link's data; otherwise, it returns None.

        Args:
            link_type (str): The type of the link to retrieve.
            targets (List[str]): A list of handles for the target nodes that the link connects to.

        Returns:
            Union[Dict[str, Any], None]: A dictionary containing the link's data if found, otherwise None.
        """
        ...

    @abstractmethod
    def get_links(
        self, link_type: str, target_types: List[str] = None, link_targets: List[str] = None
    ) -> Union[List[str], List[Dict]]:
        """
        Retrieves links of a specified type, optionally filtered by target types or specific targets.

        This method can be used to fetch links that match a given link type. Additionally, it allows
        for filtering based on the types of the targets (target_types) or the specific targets
        themselves (link_targets). If both target_types and link_targets are provided, the method
        filters using both criteria.

        Args:
            link_type (str): The type of links to retrieve. This parameter is mandatory.
            target_types (List[str], optional): A list of target types to filter the links by. If
                provided, only links that have targets of these types will be returned. Defaults to None.
            link_targets (List[str], optional): A list of specific targets to filter the links by. If
                provided, only links that have these specific targets will be returned. Defaults to None.

        Returns:
            Union[List[str], List[Dict]]: A list of links that match the criteria. The list contains
            either strings (handles of the links) or dictionaries (the link objects themselves),
            depending on the implementation details of the subclass.
        """
        ...

    @abstractmethod
    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> List[Union[dict, str, Tuple[dict, List[dict]]]]:
        """
        Retrieves incoming links for a specified atom handle, with optional filtering parameters.

        This method fetches all links pointing to the specified atom, identified by its handle. It
        can return a simple list of links, a list of dictionaries with link details, or a list of
        tuples containing link details and their respective target atoms' details, depending on the
        implementation and the provided keyword arguments.

        Args:
            atom_handle (str): The unique handle of the atom for which incoming links are to be
                               retrieved.

        Keyword Args:
            **kwargs: Arbitrary keyword arguments for filtering or specifying the format of the
                      returned data. The exact options available depend on the implementation.

        Returns:
            List[Union[dict, str, Tuple[dict, List[dict]]]]: A list containing the incoming links. The
            format of the list's elements can vary based on the provided keyword arguments and the
            implementation details.
        """
        ...

    @abstractmethod
    def query(
        self, query: Query, parameters: Optional[Dict[str, Any]] = {}
    ) -> Union[Iterator[QueryAnswer], List[QueryAnswer]]:
        """
        Executes a query against the database and returns the results.

        This method processes a given query, applying any provided parameters to modify its behavior
        or filter its results. Depending on the 'no_iterator' parameter within 'parameters', this
        method may return either a list of QueryAnswer objects or an iterator over such objects.

        Args:
            query (Query): The query to be executed. This is typically a structured query that
                           specifies what data to retrieve or what operations to perform.
            parameters (Optional[Dict[str, Any]]): A dictionary of parameters that can modify the
                           query execution or its results. For example, 'no_iterator' can be set to
                           True to return a list instead of an iterator.

        Returns:
            Union[Iterator[QueryAnswer], List[QueryAnswer]]: Depending on the 'no_iterator' parameter,
            returns either an iterator over QueryAnswer objects or a list of QueryAnswer objects.
        """
        raise NotImplementedError

    @abstractmethod
    def custom_query(
        self, index_id: str, query: Query, **kwargs
    ) -> Union[Iterator, List[Dict[str, Any]]]:
        """
        Executes a custom query based on a specific index ID and query parameters.

        This method allows for executing more complex or specific queries that are not covered
        by the standard query methods. It can return either a list of dictionaries containing
        the query results or an iterator over such dictionaries, depending on the 'no_iterator'
        parameter in kwargs.

        Args:
            index_id (str): The ID of the index to query against.
            query (Query): The query to be executed. This is typically a structured query that
                           specifies what data to retrieve or what operations to perform.

        Keyword Args:
            **kwargs: Arbitrary keyword arguments that can modify the query execution or its
                      results. For example, 'no_iterator' can be set to True to return a list
                      instead of an iterator. Other options depend on the implementation.

        Returns:
            Union[Iterator, List[Dict[str, Any]]]: Depending on the 'no_iterator' parameter, returns
            either an iterator over dictionaries containing the query results or a list of such
            dictionaries.
        """
        ...

    @abstractmethod
    def count_atoms(self) -> Tuple[int, int]:
        """
        Counts the total number of atoms in the database.

        This method aggregates the count of all atoms stored within the database, providing a
        comprehensive overview of the total number of atoms. It returns a tuple where the first
        element represents the count of node atoms, and the second element represents the count of
        link atoms.

        Returns:
            Tuple[int, int]: A tuple containing two integers, where the first integer is the count of
            node atoms and the second is the count of link atoms.
        """
        ...

    @abstractmethod
    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]]):
        """
        Reindexes the database based on the provided pattern index templates.

        This method allows for the reindexing of the database to optimize query performance based
        on specific patterns defined in the index templates. It can be used to create or update
        indexes for more efficient data retrieval.

        Args:
            pattern_index_templates (Optional[Dict[str, Dict[str, Any]]]): A dictionary where each
                key represents an index name, and its value is another dictionary specifying the
                index pattern and additional parameters. If None, the method may perform a default
                reindexing operation, depending on the implementation.
        """
        ...

    @abstractmethod
    def create_field_index(
        self,
        atom_type: str,
        fields: List[str],
        named_type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
        index_type: Optional[str] = None,
    ) -> str:
        """
        Creates an index for a specified atom type and fields, with optional parameters for further
        customization.

        This method facilitates the creation of indexes on atoms to improve query performance. It
        supports indexing on various atom types and allows for the specification of named types,
        composite types, and the index type itself for more advanced indexing strategies.

        Args:
            atom_type (str): The type of atom to index.
            fields (List[str]): The list of fields of the atom to be included in the index.
            named_type (Optional[str]): The named type of the atom, if applicable. Defaults to None.
            composite_type (Optional[List[Any]]): A list specifying the composite type of the atom,
                if applicable. Defaults to None.
            index_type (Optional[str]): The type of index to create. This can be used to specify
                different indexing strategies or algorithms. Defaults to None.

        Returns:
            str: A string identifier for the created index.
        """
        ...

    @abstractmethod
    def fetch(
        self,
        query: Query,
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs,
    ) -> Any:
        """
        Fetches data based on a given query, optionally targeting a specific host and port.

        This method is designed to execute a query and retrieve data from either a local or remote
        backend, depending on the provided host and port. If no host or port is specified, the method
        defaults to using the local backend. Additional keyword arguments can be provided to further
        customize the query execution or the data retrieval process.

        Args:
            query (Query): The query to be executed. This is typically a structured query that
                           specifies what data to retrieve or what operations to perform.
            host (Optional[str]): The host of the remote backend to query. If None, the local backend
                                  is used. Defaults to None.
            port (Optional[int]): The port of the remote backend to query. This parameter is ignored
                                  if the host is None. Defaults to None.

        Keyword Args:
            **kwargs: Arbitrary keyword arguments that can modify the query execution or its results.
                      These options depend on the implementation of the backend.

        Returns:
            Any: The result of the query execution. The exact type and structure of the result depend
                 on the implementation of the backend and the nature of the query.
        """
        ...

    @abstractmethod
    def create_context(self, name: str, queries: Optional[List[Query]]) -> Context:
        """
        Creates a new context with a specified name and an optional list of queries.

        This method initializes a new context object that can be used to manage and execute a
        collection of queries within a defined scope. The context can encapsulate a specific
        computational or data retrieval task, grouping related queries for efficient management
        and execution.

        Args:
            name (str): The name of the context. This should be unique within the scope of the
                        application or dataset.
            queries (Optional[List[Query]]): A list of queries to be associated with the context.
                                             These queries can be executed within the context's
                                             scope. Defaults to None, indicating no initial queries.

        Returns:
            Context: An instance of the Context class, initialized with the specified name and
                     queries.
        """
        ...

    @abstractmethod
    def commit(self, **kwargs) -> None:
        """
        Commits changes to the database.

        This method is responsible for committing any pending changes to the database. Depending on the
        implementation, this could involve flushing a local buffer to a remote database, or directly
        committing changes to a local database. The behavior can be modified by passing specific keyword
        arguments.

        Keyword Args:
            **kwargs: Arbitrary keyword arguments that can influence the commit operation. The exact
                      options available depend on the implementation. For example, a 'buffer' keyword
                      could be used to specify a particular set of changes to commit.
        """
        ...

    @abstractmethod
    def get_atoms_by_field(self, query: Query) -> List[str]:
        """
        Retrieves a list of atom handles based on a specified field query.

        This method executes a query that filters atoms by specific field values. It returns a list
        of atom handles that match the query criteria. The query should specify the field and value(s)
        to filter by.

        Args:
            query (Query): The query specifying the field and value(s) to filter atoms by.

        Returns:
            List[str]: A list of atom handles that match the query criteria.
        """
        ...

    @abstractmethod
    def get_atoms_by_text_field(
        self, text_value: str, field: Optional[str] = None, text_index_id: Optional[str] = None
    ) -> List[str]:
        """
        Retrieves a list of atom handles based on a text field value, with optional field and index ID.

        This method is designed to search for atoms where a specified text field matches the given
        text value. It allows for an optional field specification and the use of a specific text index
        ID for more efficient querying.

        Args:
            text_value (str): The text value to search for within the specified field.
            field (Optional[str]): The name of the field to search. If None, a default or all-encompassing
                                   text search may be performed, depending on the implementation.
            text_index_id (Optional[str]): The ID of the text index to use for the search. This can
                                           optimize the search process if provided. Defaults to None.

        Returns:
            List[str]: A list of atom handles that match the search criteria.
        """
        ...

    @abstractmethod
    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> List[str]:
        """
        Retrieves a list of node handles where the node name starts with a specified string.

        This method searches for nodes of a specific type whose names begin with the given 'startswith'
        string. It returns a list of handles for the nodes that match these criteria.

        Args:
            node_type (str): The type of the nodes to search for.
            startswith (str): The initial string of the node names to match.

        Returns:
            List[str]: A list of handles for the nodes that match the search criteria.
        """
        ...


class LocalQueryEngine(QueryEngine):
    def __init__(
        self, backend, system_parameters: Dict[str, Any], kwargs: Optional[dict] = {}
    ) -> None:
        self.system_parameters = system_parameters
        self.local_backend = backend

    def _recursive_query(
        self,
        query: Query,
        mappings: Set[Assignment] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> QueryAnswerIterator:
        if isinstance(query, list):
            sub_expression_results = [
                self._recursive_query(expression, mappings, parameters) for expression in query
            ]
            return AndEvaluator(sub_expression_results)
        elif query["atom_type"] == "node":
            try:
                atom_handle = self.local_backend.get_node_handle(query["type"], query["name"])
                return ListIterator([QueryAnswer(self.local_backend.get_atom(atom_handle), None)])
            except NodeDoesNotExist:
                return ListIterator([])
        elif query["atom_type"] == "link":
            matched_targets = []
            for target in query["targets"]:
                if target["atom_type"] == "node" or target["atom_type"] == "link":
                    matched = self._recursive_query(target, mappings, parameters)
                    if matched:
                        matched_targets.append(matched)
                elif target["atom_type"] == "variable":
                    matched_targets.append(ListIterator([QueryAnswer(target, None)]))
                else:
                    das_error(
                        UnexpectedQueryFormat(
                            message="Query processing reached an unexpected state",
                            details=f'link: {str(query)} link target: {str(query)}',
                        )
                    )
            return LazyQueryEvaluator(query["type"], matched_targets, self, parameters)
        else:
            das_error(
                UnexpectedQueryFormat(
                    message="Query processing reached an unexpected state",
                    details=f'query: {str(query)}',
                )
            )

    def _to_link_dict_list(self, db_answer: Union[List[str], List[Dict]]) -> List[Dict]:
        if not db_answer:
            return []
        flat_handle = isinstance(db_answer[0], str)
        answer = []
        for atom in db_answer:
            handle = atom if flat_handle else atom[0]
            answer.append(self.local_backend.get_atom(handle))
        return answer

    def _get_related_links(
        self,
        link_type: str,
        target_types: List[str] = None,
        link_targets: List[str] = None,
        **kwargs,
    ):
        if link_type != WILDCARD and target_types is not None:
            return self.local_backend.get_matched_type_template(
                [link_type, *target_types], **kwargs
            )
        elif link_targets is not None:
            try:
                return self.local_backend.get_matched_links(link_type, link_targets, **kwargs)
            except LinkDoesNotExist:
                return None, [] if kwargs.get('cursor') is not None else []
        elif link_type != WILDCARD:
            return self.local_backend.get_all_links(link_type, **kwargs)
        else:
            das_error(
                ValueError(
                    f"Invalid parameters. link_type = {link_type} target_types = {target_types} link_targets = {link_targets}"
                )
            )

    def _process_node(self, query: dict) -> List[dict]:
        try:
            handle = self.local_backend.node_handle(query["type"], query["name"])
            return [self.local_backend.get_atom(handle, no_target_format=True)]
        except AtomDoesNotExist:
            return []

    def _process_link(self, query: dict) -> List[dict]:
        target_handles = self._generate_target_handles(query['targets'])
        matched_links = self.local_backend.get_matched_links(
            link_type=query["type"], target_handles=target_handles
        )
        unique_handles = set()
        result = []

        for link in matched_links:
            if isinstance(link, str):  # single link
                link_handle = link
                link_targets = target_handles
            else:
                link_handle, *link_targets = link

            if link_handle not in unique_handles:
                unique_handles.add(link_handle)
                result.append(self.local_backend.get_atom(link_handle, no_target_format=True))

            for target in link_targets:
                atoms = self._handle_to_atoms(target)
                if isinstance(atoms, list):
                    for atom in atoms:
                        if atom['_id'] not in unique_handles:
                            unique_handles.add(atom['_id'])
                            result.append(atom)
                else:
                    if atoms['_id'] not in unique_handles:
                        unique_handles.add(atoms['_id'])
                        result.append(atoms)

        return result

    def _generate_target_handles(self, targets: List[Dict[str, Any]]) -> List[str]:
        targets_hash = []
        for target in targets:
            if target["atom_type"] == "node":
                handle = self.local_backend.node_handle(target["type"], target["name"])
            elif target["atom_type"] == "link":
                handle = self._generate_target_handles(target)
            elif target["atom_type"] == "variable":
                handle = WILDCARD
            targets_hash.append(handle)
        return targets_hash

    def _handle_to_atoms(self, handle: str) -> Union[List[dict], dict]:
        try:
            atom = self.local_backend.get_atom(handle, no_target_format=True)
        except AtomDoesNotExist:
            return []

        if 'name' in atom:  # node
            return atom
        else:  # link
            answer = [atom]
            for key, value in atom.items():
                if re.search(AtomDB.key_pattern, key):
                    answer.append(self._handle_to_atoms(value))
            return answer

    def has_buffer(self) -> bool:
        if isinstance(self.local_backend, InMemoryDB):
            atoms = self.local_backend.db.node
            atoms.update(self.local_backend.db.link)
            self.buffer = list(atoms.values())
            return bool(atoms)
        return False

    def get_atom(self, handle: str, **kwargs) -> Union[Dict[str, Any], None]:
        try:
            return self.local_backend.get_atom(handle, **kwargs)
        except AtomDoesNotExist as exception:
            das_error(exception)

    def get_node(self, node_type: str, node_name: str) -> Union[Dict[str, Any], None]:
        try:
            node_handle = self.local_backend.node_handle(node_type, node_name)
            return self.local_backend.get_atom(node_handle)
        except AtomDoesNotExist:
            das_error(
                NodeDoesNotExist(message="Nonexistent node.", details=f'{node_type}:{node_name}')
            )

    def get_link(self, link_type: str, link_targets: List[str]) -> Union[Dict[str, Any], None]:
        try:
            link_handle = self.local_backend.link_handle(link_type, link_targets)
            return self.local_backend.get_atom(link_handle)
        except AtomDoesNotExist:
            das_error(
                LinkDoesNotExist(message='Nonexistent link', details=f'{link_type}:{link_targets}')
            )

    def get_links(
        self,
        link_type: str,
        target_types: List[str] = None,
        link_targets: List[str] = None,
        **kwargs,
    ) -> Union[Iterator, List[str], List[Dict]]:
        if kwargs.get('no_iterator', True):
            answer = self._get_related_links(link_type, target_types, link_targets, **kwargs)
            if not answer:
                return []
            if isinstance(answer, tuple):
                if isinstance(answer[0], int):
                    return answer[0], self._to_link_dict_list(answer[1])
                else:
                    return self._to_link_dict_list(answer[1])
            return self._to_link_dict_list(answer)
        else:
            if kwargs.get('cursor') is None:
                kwargs['cursor'] = 0
            answer = self._get_related_links(link_type, target_types, link_targets, **kwargs)
            kwargs['backend'] = self
            kwargs['link_type'] = link_type
            kwargs['target_types'] = target_types
            kwargs['link_targets'] = link_targets
            if isinstance(answer, tuple):  # redis_mongo use case
                kwargs['cursor'] = answer[0]
                answer = answer[1]
            return LocalGetLinks(ListIterator(answer), **kwargs)

    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> Union[Iterator, List[Union[dict, str, Tuple[dict, List[dict]]]]]:
        if kwargs.get('no_iterator', True):
            return self.local_backend.get_incoming_links(atom_handle, **kwargs)
        else:
            kwargs['handles_only'] = True
            links = self.local_backend.get_incoming_links(atom_handle, **kwargs)
            kwargs['backend'] = self.local_backend
            kwargs['atom_handle'] = atom_handle
            if isinstance(links, tuple):  # redis_mongo use case
                kwargs['cursor'] = links[0]
                links = links[1]
            return LocalIncomingLinks(ListIterator(links), **kwargs)

    def query(
        self,
        query: Query,
        parameters: Optional[Dict[str, Any]] = {},
    ) -> Union[Iterator[QueryAnswer], List[QueryAnswer]]:
        no_iterator = parameters.get("no_iterator", False)
        if no_iterator:
            logger().debug(
                {
                    'message': '[DistributedAtomSpace][query] - Start',
                    'data': {'query': query, 'parameters': parameters},
                }
            )
        query_results = self._recursive_query(query, parameters)
        if no_iterator:
            answer = [result for result in query_results]
            logger().debug(f"query: {query} result: {str(answer)}")
            return answer
        else:
            return query_results

    def custom_query(
        self, index_id: str, query: Query, **kwargs
    ) -> Union[Iterator, List[Dict[str, Any]]]:
        if kwargs.pop('no_iterator', True):
            return self.local_backend.get_atoms_by_index(index_id, query=query, **kwargs)
        else:
            if kwargs.get('cursor') is None:
                kwargs['cursor'] = 0
            cursor, answer = self.local_backend.get_atoms_by_index(index_id, query=query, **kwargs)
            kwargs['backend'] = self.local_backend
            kwargs['index_id'] = index_id
            kwargs['cursor'] = cursor
            return CustomQuery(ListIterator(answer), **kwargs)

    def count_atoms(self) -> Tuple[int, int]:
        return self.local_backend.count_atoms()

    def commit(self, **kwargs) -> None:
        if kwargs.get('buffer'):
            self.local_backend.commit(buffer=kwargs['buffer'])
        self.local_backend.commit()

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]] = None):
        self.local_backend.reindex(pattern_index_templates)

    def create_field_index(
        self,
        atom_type: str,
        fields: List[str],
        named_type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
        index_type: Optional[str] = None,
    ) -> str:
        return self.local_backend.create_field_index(
            atom_type, fields, named_type, composite_type, index_type
        )

    def fetch(
        self,
        query: Query,
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs,
    ) -> Any:
        if not self.system_parameters.get('running_on_server'):  # Local
            if host is not None and port is not None:
                server = FunctionsClient(host, port)
            else:
                server = self.local_backend
            return server.fetch(query=query, **kwargs)
        else:
            if query is None:
                try:
                    return self.local_backend.retrieve_all_atoms()
                except Exception as e:
                    das_error(e)
            else:
                if 'atom_type' not in query:
                    das_error(ValueError('Invalid query: missing atom_type'))

                atom_type = query['atom_type']

                if atom_type == 'node':
                    return self._process_node(query)
                elif atom_type == 'link':
                    return self._process_link(query)
                else:
                    das_error(
                        ValueError("Invalid atom type: {atom_type}. Use 'node' or 'link' instead.")
                    )

    def create_context(
        self,
        name: str,
        queries: Optional[List[Query]] = None,
    ) -> Context:
        das_error(NotImplementedError("Contexts are not implemented for non-server local DAS"))

    def get_atoms_by_field(self, query: Query) -> List[str]:
        return self.local_backend.get_atoms_by_field(query)

    def get_atoms_by_text_field(
        self, text_value: str, field: Optional[str] = None, text_index_id: Optional[str] = None
    ) -> List[str]:
        return self.local_backend.get_atoms_by_text_field(text_value, field, text_index_id)

    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> List[str]:
        return self.local_backend.get_node_by_name_starting_with(node_type, startswith)


class RemoteQueryEngine(QueryEngine):
    def __init__(self, backend, system_parameters: Dict[str, Any], kwargs: Optional[dict] = {}):
        self.system_parameters = system_parameters
        self.local_query_engine = LocalQueryEngine(backend, kwargs)
        self.__mode = kwargs.get('mode', 'read-only')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        if not self.host or not self.port:
            das_error(InvalidDASParameters(message="'host' and 'port' are mandatory parameters"))
        self.remote_das = FunctionsClient(self.host, self.port)

    @property
    def mode(self):
        return self.__mode

    def get_atom(self, handle: str, **kwargs) -> Dict[str, Any]:
        try:
            atom = self.local_query_engine.get_atom(handle, **kwargs)
        except AtomDoesNotExist:
            try:
                atom = self.remote_das.get_atom(handle, **kwargs)
            except AtomDoesNotExist as exception:
                das_error(exception)
        return atom

    def get_node(self, node_type: str, node_name: str) -> Dict[str, Any]:
        try:
            node = self.local_query_engine.get_node(node_type, node_name)
        except NodeDoesNotExist:
            try:
                node = self.remote_das.get_node(node_type, node_name)
            except NodeDoesNotExist as exception:
                das_error(exception)
        return node

    def get_link(self, link_type: str, link_targets: List[str]) -> Dict[str, Any]:
        try:
            link = self.local_query_engine.get_link(link_type, link_targets)
        except LinkDoesNotExist:
            try:
                link = self.remote_das.get_link(link_type, link_targets)
            except LinkDoesNotExist as exception:
                das_error(exception)
        return link

    def get_links(
        self,
        link_type: str,
        target_types: List[str] = None,
        link_targets: List[str] = None,
        **kwargs,
    ) -> Union[Iterator, List[str], List[Dict]]:
        kwargs.pop('no_iterator', None)
        if kwargs.get('cursor') is None:
            kwargs['cursor'] = 0
        links = self.local_query_engine.get_links(link_type, target_types, link_targets, **kwargs)
        cursor, remote_links = self.remote_das.get_links(
            link_type, target_types, link_targets, **kwargs
        )
        kwargs['cursor'] = cursor
        kwargs['backend'] = self.remote_das
        kwargs['link_type'] = link_type
        kwargs['target_types'] = target_types
        kwargs['link_targets'] = link_targets
        links.extend(remote_links)
        return RemoteGetLinks(ListIterator(links), **kwargs)

    def get_incoming_links(self, atom_handle: str, **kwargs) -> Iterator:
        kwargs.pop('no_iterator', None)
        if kwargs.get('cursor') is None:
            kwargs['cursor'] = 0
        kwargs['handles_only'] = False
        links = self.local_query_engine.get_incoming_links(atom_handle, **kwargs)
        cursor, remote_links = self.remote_das.get_incoming_links(atom_handle, **kwargs)
        kwargs['cursor'] = cursor
        kwargs['backend'] = self.remote_das
        kwargs['atom_handle'] = atom_handle
        links.extend(remote_links)
        return RemoteIncomingLinks(ListIterator(links), **kwargs)

    def custom_query(self, index_id: str, query: Query, **kwargs) -> Iterator:
        kwargs.pop('no_iterator', None)
        if kwargs.get('cursor') is None:
            kwargs['cursor'] = 0
        cursor, answer = self.remote_das.custom_query(index_id, query=query, **kwargs)
        kwargs['backend'] = self.remote_das
        kwargs['index_id'] = index_id
        kwargs['cursor'] = cursor
        kwargs['is_remote'] = True
        return CustomQuery(ListIterator(answer), **kwargs)

    def query(
        self,
        query: Query,
        parameters: Optional[Dict[str, Any]] = {},
    ) -> Union[Iterator[QueryAnswer], List[QueryAnswer]]:
        query_scope = parameters.get('query_scope', 'remote_only')
        if query_scope == 'remote_only' or query_scope == 'synchronous_update':
            if query_scope == 'synchronous_update':
                self.commit()
            parameters['no_iterator'] = True
            answer = self.remote_das.query(query, parameters)
        elif query_scope == 'local_only':
            answer = self.local_query_engine.query(query, parameters)
        elif query_scope == 'local_and_remote':
            das_error(
                QueryParametersException(
                    message=f"Invalid value for parameter 'query_scope': '{query_scope}'. This type of query scope is not implemented yet"
                )
            )
        else:
            das_error(
                QueryParametersException(
                    message=f'Invalid value for "query_scope": "{query_scope}"'
                )
            )
        return answer

    def count_atoms(self) -> Tuple[int, int]:
        local_answer = self.local_query_engine.count_atoms()
        remote_answer = self.remote_das.count_atoms()
        return tuple([x + y for x, y in zip(local_answer, remote_answer)])

    def commit(self, **kwargs) -> None:
        if self.__mode == 'read-write':
            if self.local_query_engine.has_buffer():
                return self.remote_das.commit_changes(buffer=self.local_query_engine.buffer)
            return self.remote_das.commit_changes()
        elif self.__mode == 'read-only':
            das_error(PermissionError("Commit can't be executed in read mode"))
        else:
            das_error(ValueError("Invalid mode: '{self.__mode}'. Use 'read-only' or 'read-write'"))

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]]):
        das_error(NotImplementedError())

    def create_field_index(
        self,
        atom_type: str,
        fields: List[str],
        named_type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
        index_type: Optional[str] = None,
    ) -> str:
        return self.remote_das.create_field_index(
            atom_type, fields, named_type, composite_type, index_type
        )

    def fetch(
        self,
        query: Query,
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs,
    ) -> Any:
        if host is not None and port is not None:
            server = FunctionsClient(host, port)
        else:
            server = self.remote_das
        return server.fetch(query=query, **kwargs)

    def create_context(
        self,
        name: str,
        queries: Optional[List[Query]] = None,
    ) -> Context:
        return self.remote_das.create_context(name, queries)

    def get_atoms_by_field(self, query: Query) -> List[str]:
        return self.remote_das.get_atoms_by_field(query)

    def get_atoms_by_text_field(
        self, text_value: str, field: Optional[str] = None, text_index_id: Optional[str] = None
    ) -> List[str]:
        return self.remote_das.get_atoms_by_text_field(text_value, field, text_index_id)

    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> List[str]:
        return self.remote_das.get_node_by_name_starting_with(node_type, startswith)
