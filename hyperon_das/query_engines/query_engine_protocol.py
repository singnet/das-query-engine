from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from hyperon_das.context import Context
from hyperon_das.type_alias import Query
from hyperon_das.utils import QueryAnswer


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
    def count_atoms(self, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        """
        Counts the total number of atoms in the database.

        This method aggregates the count of all atoms stored within the database, providing a
        comprehensive overview of the total number of atoms. It returns a dict where the key 'node_count'  represents
        the count of node atoms, the key 'link_count' represents the count of link atoms, the key 'atom_count'
        represents the total of atoms .

        Returns:
            Dict[str, int]: A dict containing str keys and integers, where the key 'node_count' has an integer which is
            the count of node atoms, the key 'link_count' is the count of link atoms and 'atom_count' is the count of
            all atoms.
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
