from typing import Any, Dict, List, Optional, Tuple, Union

from hyperon_das.constants import DasType
from hyperon_das.exceptions import DasTypeException, InitializeDasServerException
from hyperon_das.factory import DasFactory, das_factory
from hyperon_das.logger import logger
from hyperon_das.pattern_matcher import LogicalExpression
from hyperon_das.utils import QueryOutputFormat


class DistributedAtomSpace:
    def __init__(self, das_type: DasType = DasType.CLIENT.value, **kwargs) -> None:
        self._type = das_type
        self._validate_type()
        self.das = self._initialize_das(kwargs)

        logger().debug(
            {
                'message': '[DistributedAtomSpace][__init__]',
                'data': {'das_type': das_type},
            }
        )

    @property
    def remote_das(self):
        return self.das.remote_das if self._type == DasType.CLIENT.value else None

    def _validate_type(self) -> None:
        try:
            DasType(self._type)
        except ValueError as e:
            self._error(
                DasTypeException(
                    message=str(e),
                    details=f'possible values {DasType.types()}',
                )
            )

    def _initialize_das(self, kwargs):
        try:
            return das_factory(DasFactory(self._type), kwargs)
        except Exception as e:
            self._error(
                InitializeDasServerException(
                    message='An error occurred during class initialization',
                    details=str(e),
                )
            )

    def clear_database(self) -> None:
        """Clear all data"""
        return self.das.clear_database()

    def count_atoms(self) -> Tuple[int, int]:
        """
        This method is useful for returning the count of atoms in the database.
        It's also useful for ensuring that the knowledge base load went off without problems.

        Returns:
            Tuple[int, int]: (node_count, link_count)
        """
        return self.das.count_atoms()

    def get_atom(
        self,
        handle: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        """
        Retrieve information about an Atom using its handle.

        This method retrieves information about an Atom from the database
        based on the provided handle. The retrieved atom information can be
        presented in different output formats as specified by the output_format parameter.

        Args:
            handle (str): The unique handle of the atom.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[str, Dict]: Depending on the output_format, returns either:
                - A string representing the handle of the Atom (output_format == QueryOutputFormat.HANDLE),
                - A dictionary containing detailed Atom information (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the Atom (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Example:
            >>> result = obj.get_atom(
                    handle="af12f10f9ae2002a1607ba0b47ba8407",
                    output_format=QueryOutputFormat.ATOM_INFO
                )
            >>> print(result)
            {
                "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                "type": "Concept",
                "name": "human"
            }
        """
        return self.das.get_atom(handle, output_format)

    def get_node(
        self,
        node_type: str,
        node_name: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        """
        Retrieve information about a Node of a specific type and name.

        This method retrieves information about a Node from the database
        based on its type and name. The retrieved node information can be
        presented in different output formats as specified by the output_format parameter.

        Args:
            node_type (str): The type of the node being queried.
            node_name (str): The name of the specific node being queried.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[str, Dict]: Depending on the output_format, returns either:
                - A string representing the handle of the node (output_format == QueryOutputFormat.HANDLE),
                - A dictionary containing atom information of the node (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the node (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Note:
            If the specified node does not exist, a warning is logged and None is returned.

        Example:
            >>> result = obj.get_node(
                    node_type='Concept',
                    node_name='human',
                    output_format=QueryOutputFormat.ATOM_INFO
                )
            >>> print(result)
            {
                "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                "type": "Concept",
                "name": "human"
            }
        """
        return self.das.get_node(node_type, node_name, output_format)

    def get_nodes(
        self,
        node_type: str,
        node_name: str = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[List[str], List[Dict]]:
        """
        Retrieve information about Nodes based on their type and optional name.

        This method retrieves information about nodes from the database based
        on its type and name (if provided). The retrieved nodes information can be
        presented in different output formats as specified by the output_format parameter.


        Args:
            node_type (str): The type of nodes being queried.
            node_name (str, optional): The name of the specific node being queried. Defaults to None.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[List[str], List[Dict]]: Depending on the output_format, returns either:
                - A list of strings representing handles of the nodes (output_format == QueryOutputFormat.HANDLE),
                - A list of dictionaries containing atom information of the nodes (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the nodes (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Note:
            If node_name is provided and the specified node does not exist, an empty list is returned.

        Example:
            >>> result = obj.get_nodes(
                    node_type='Concept',
                    output_format=QueryOutputFormat.HANDLE
                )
            >>> print(result)
            [
                'af12f10f9ae2002a1607ba0b47ba8407',
                '1cdffc6b0b89ff41d68bec237481d1e1',
                '5b34c54bee150c04f9fa584b899dc030',
                'c1db9b517073e51eb7ef6fed608ec204',
                ...
            ]
        """
        return self.das.get_nodes(node_type, node_name, output_format)

    def get_link(
        self,
        link_type: str,
        targets: List[str],
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        """
        Retrieve information about a link of a specific type and its targets.

        This method retrieves information about a link from the database based on
        type with given targets. The retrieved link information can be presented in different
        output formats as specified by the output_format parameter.

        Args:
            link_type (str): The type of the link being queried.
            targets (List[str], optional): A list of target identifiers that the link is associated with.
                Defaults to None.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[str, Dict]: Depending on the output_format, returns either:
                - A string representing the handle of the link (output_format == QueryOutputFormat.HANDLE),
                - A dictionary containing atom information of the link (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the link (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Note:
            If the specified link or targets do not exist, the method returns None.

        Example:
            >>> result = obj.get_link(
                    link_type='Similarity',
                    targets=['human', 'monkey'],
                    output_format=QueryOutputFormat.HANDLE
                )
            >>> print(result)
            '2931276cb5bb4fc0c2c48a6720fc9a84'
        """
        return self.das.get_link(link_type, targets, output_format)

    def get_links(
        self,
        link_type: str,
        target_types: str = None,
        targets: List[str] = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[List[str], List[Dict]]:
        """
        Retrieve information about Links based on specified criteria.

        This method retrieves information about links from the database based on the provided criteria.
        The criteria includes the link type, and can include target types and specific target identifiers.
        The retrieved links information can be presented in different output formats as specified
        by the output_format parameter.

        Args:
            link_type (str): The type of links being queried.
            target_types (str, optional): The type(s) of targets being queried. Defaults to None.
            targets (List[str], optional): A list of target identifiers that the links are associated with.
                Defaults to None.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[List[str], List[Dict]]: Depending on the output_format, returns either:
                - A list of strings representing handles of the links (output_format == QueryOutputFormat.HANDLE),
                - A list of dictionaries containing detailed information of the links (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the links (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided or if the provided parameters are invalid.

        Example:
            >>> result = obj.get_links(
                    link_type='Similarity',
                    target_types=['Concept', 'Concept'],
                    output_format=QueryOutputFormat.ATOM_INFO
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

    def get_link_type(self, link_handle: str) -> str:
        """
        Get the type of a link.

        This method retrieves the type of a link based on its handle.

        Args:
            link_handle (str): The handle of the link.

        Returns:
            str: The type of the link.

        Example:
            >>> human = obj.get_node('Concept', 'human')
            >>> monkey = obj.get_node('Concept', 'monkey')
            >>> link_handle = obj.get_link('Similarity', [human, monkey])
            >>> result = obj.get_link_type(link_handle=link_handle)
            >>> print(result)
            'Similarity'
        """
        return self.das.get_link_type(link_handle)

    def get_link_targets(self, link_handle: str) -> List[str]:
        """
        Get the targets of a link.

        This method retrieves the targets of a link based on its handle.

        Args:
            link_handle (str): The handle of the link.

        Returns:
            List[str]: A list of target handles.

        Example:
            >>> human = obj.get_node('Concept', 'human')
            >>> monkey = obj.get_node('Concept', 'monkey')
            >>> link_handle = obj.get_link('Similarity', [human, monkey])
            >>> result = obj.get_link_targets(link_handle=link_handle)
            >>> print(result)
            [
                '80aff30094874e75028033a38ce677bb',
                '4e8e26e3276af8a5c2ac2cc2dc95c6d2'
            ]
        """
        return self.das.get_link_targets(link_handle)

    def get_node_type(self, node_handle: str) -> str:
        """
        Get the type of a node.

        This method retrieves the type of a node based on its handle.

        Args:
            node_handle (str): The handle of the node.

        Returns:
            str: The type of the node.

        Example:
            >>> human = obj.get_node('Concept', 'human')
            >>> result = obj.get_node_type(node_handle=human)
            >>> print(result)
            'Concept'
        """
        return self.das.get_node_type(node_handle)

    def get_node_name(self, node_handle: str) -> str:
        """
        Get the name of a node.

        This method retrieves the name of a node based on its handle.

        Args:
            node_handle (str): The handle of the node.

        Returns:
            str: The name of the node.

        Example:
            >>> animal = obj.get_node('Concept', 'animal')
            >>> result = obj.get_node_name(node_handle=animal)
            >>> print(result)
            'animal'
        """
        return self.das.get_node_name(node_handle)

    def query(
        self,
        query: Dict[str, Any],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Perform a query on the knowledge base using a dict as input. Returns a
        list of dicts as result.

        The input dict is a link, used as a pattern to make the query.
        Variables can be used as link targets as well as nodes. Nested links are
        allowed as well.

        Args:
            query (Dict[str, Any]): A pattern described as a link (possibly with nested links) with
            nodes and variables used to query the knoeledge base.
            extra_paramaters (Dict[str, Any], optional): query optional parameters

        Returns:
            List[Dict[str, Any]]: a list of dicts with the matching subgraphs

        Raises:
            UnexpectedQueryFormat: If query resolution lead to an invalid state

        Notes:
            - No logical connectors (AND, OR, NOT) are allowed
            - If no match is found for the query, an empty list is returned.

        Example:

            >>> hash_table_api.add_link({
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
            >>> query_params = {
                "toplevel_only": False,
                "return_type": QueryOutputFormat.ATOM_INFO,
            }
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
            >>> result = hash_table_api.query(q1, query_params)
            >>> print(result)
            [{'handle': 'dbcf1c7b610a5adea335bf08f6509978', 'type': 'Expression', 'template': ['Expression', 'Symbol', ['Expression', 'Symbol', 'Symbol']], 'targets': [{'handle': '963d66edfb77236054125e3eb866c8b5', 'type': 'Symbol', 'name': 'Test'}, {'handle': '233d9a6da7d49d4164d863569e9ab7b6', 'type': 'Expression', 'template': ['Expression', 'Symbol', 'Symbol'], 'targets': [{'handle': '963d66edfb77236054125e3eb866c8b5', 'type': 'Symbol', 'name': 'Test'}, {'handle': '9f27a331633c8bc3c49435ffabb9110e', 'type': 'Symbol', 'name': '2'}]}]}]
        """
        logger().debug(
            {
                'message': '[DistributedAtomSpace][query] - Start',
                'data': {'query': query, 'extra_parameters': extra_parameters},
            }
        )
        try:
            return self.das.query(query, extra_parameters)
        except Exception as e:
            self._error(e)

    def pattern_matcher_query(
        self,
        query: LogicalExpression,
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> dict | list | None:
        """
        Perform a query on the knowledge base using a logical expression.

        This method allows you to query the knowledge base using a logical expression
        to find patterns or relationships among atoms. The query result is returned
        in the specified output format.

        Args:
            query (LogicalExpression): As instance of a LogicalExpression. representing the query.
            output_format (QueryOutputFormat, optional): The desired output format for the query result
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[Dict[str, Any]], List]: Depending on the `return_type` parameter sent in extra_parameters, returns:
                - A list of dictionaries (return_type == QueryOutputFormat.HANDLE or return_type == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the links (return_type == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Notes:
            - Each query is a LogicalExpression object that may or may not be a combination of
            logical operators like `And`, `Or`, and `Not`, as well as atomic expressions like
            `Node`, `Link`, and `Variable`.

            - If no match is found for the query, an empty string is returned.

        Example:
            You can use this method to perform complex or simple queries, like the following:

            In this example we want to search the knowledge base for two inheritance links
            that connect 3 nodes such that V1 -> V2 -> V3.

            >>> V1 = Variable("V1")
            >>> V2 = Variable("V2")
            >>> V3 = Variable("V3")

            >>> logical_expression = And([
                Link("Inheritance", ordered=True, targets=[V1, V2]),
                Link("Inheritance", ordered=True, targets=[V2, V3])
            ])

            >>> result = obj.query(query=logical_expression, {'return_type': QueryOutputFormat.HANDLE})

            >>> print(result)
            {
                {'V1': '305e7d502a0ce80b94374ff0d79a6464', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'},
                {'V1': 'bd497eb24420dd50fed5f3d2e6cdd7c1', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'},
                {'V1': 'e2d9b15ab3461228d75502e754137caa', 'V2': 'c90242e2dbece101813762cc2a83d726', 'V3': '81ec21b0f1b03e18c55e056a56179fef'},
                ...
            }
        """

        logger().debug(
            {
                'message': '[DistributedAtomSpace][pattern_matcher_query] - Start',
                'data': {'query': query, 'extra_parameters': extra_parameters},
            }
        )
        return self.das.pattern_matcher_query(query, extra_parameters)

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        return self.das.add_node(node_params)

    def add_link(self, link_params: Dict[str, Any]) -> Dict[str, Any]:
        return self.das.add_link(link_params)

    def attach_remote(
        self, host: str, port: Optional[str] = None, name: Optional[str] = None
    ) -> bool:
        """
        Establish a connection to a remote server and attach a server instance.

        Args:
            host (str): The hostname or IP address of the remote server.
            port (Optional[str]): The port number to connect to on the remote server. Defaults to None.
            name (Optional[str]): A user-defined name for the FunctionsClient instance. Defaults to None.

        Returns:
            bool: Returns True if the attachment process is successful, otherwise False.

        Example:
            Use this method to attach to a remote server:

            >>> instance = DistributedAtomSpace()
            >>> result = instance.attach_remote(host="1.2.3.4", port="1234", name="RemoteServer1")
        """
        return self.das.attach_remote(host, port, name)


if __name__ == '__main__':
    das = DistributedAtomSpace()

    link = {
        'type': 'Inheritance',
        'targets': [
            {'type': 'Concept', 'name': 'marco', 'weight': 0.5},
            {'type': 'Concept', 'name': 'mammal'},
        ],
        'color': 'blue',
    }

    das.add_link(link)

    query = {
        "atom_type": "link",
        "type": "Inheritance",
        "targets": [
            {"atom_type": "variable", "name": "v1"},
            {"atom_type": "node", "type": "Concept", "name": "mammal"},
        ],
    }
    query_params = {
        "toplevel_only": False,
        "return_type": QueryOutputFormat.ATOM_INFO,
    }
    result = das.query(query, query_params)

    print(result)
