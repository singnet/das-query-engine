import json
from typing import Any, Dict, List, Tuple, Union

from hyperon_das_atomdb import WILDCARD

from hyperon_das.das import DistributedAtomSpace, QueryOutputFormat
from hyperon_das.exceptions import DatabaseTypeException, MethodNotAllowed
from hyperon_das.factory import DatabaseFactory, DatabaseType, database_factory
from hyperon_das.logger import logger
from hyperon_das.pattern_matcher import (
    LogicalExpression,
    PatternMatchingAnswer,
)


class DistributedAtomSpaceAPI(DistributedAtomSpace):
    def __init__(self, database: DatabaseType) -> None:
        self._db_type = database
        try:
            DatabaseType(database)
        except ValueError as e:
            raise DatabaseTypeException(
                message=str(e),
                details=f'possible values {DatabaseType.values()}',
            )
        self.db = database_factory(DatabaseFactory(self._db_type))
        self.pattern_black_list = []
        logger().info(
            f"New Distributed Atom Space. Database name: {self.db.database_name}"
        )

    def clear_database(self) -> None:
        """Clear all data"""
        return self.db.clear_database()

    def count_atoms(self) -> Tuple[int, int]:
        """
        This method is useful for returning the count of atoms in the database.
        It's also useful for ensuring that the knowledge base load went off without problems.

        Returns:
            Tuple[int, int]: (node_count, link_count)
        """
        return self.db.count_atoms()

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

        if output_format == QueryOutputFormat.HANDLE or not handle:
            atom = self.db.get_atom_as_dict(handle)
            return atom["handle"] if atom else ""
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(handle)
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(handle)
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

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

        node_handle = None

        try:
            node_handle = self.db.get_node_handle(node_type, node_name)
        except ValueError:
            logger().warning(
                f"Attempt to access an invalid Node '{node_type}:{node_name}'"
            )
            return None

        if output_format == QueryOutputFormat.HANDLE or not node_handle:
            return node_handle
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(node_handle)
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(node_handle)
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

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

        if node_name is not None:
            answer = self.db.get_node_handle(node_type, node_name)
            if answer is not None:
                answer = [answer]
        else:
            answer = self.db.get_all_nodes(node_type)

        if output_format == QueryOutputFormat.HANDLE or not answer:
            return answer
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return [self.db.get_atom_as_dict(handle) for handle in answer]
        elif output_format == QueryOutputFormat.JSON:
            answer = [
                self.db.get_atom_as_deep_representation(handle)
                for handle in answer
            ]
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

    def get_link(
        self,
        link_type: str,
        targets: List[str] = None,
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
        link_handle = None

        # TODO: Is there any exception action?
        try:
            link_handle = self.db.get_link_handle(link_type, targets)
        except Exception as e:
            raise e

        if output_format == QueryOutputFormat.HANDLE or link_handle is None:
            return link_handle
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(link_handle, len(targets))
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(
                link_handle, len(targets)
            )
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

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

        # TODO: Delete this If. This conditional will never happen
        if link_type is None:
            link_type = WILDCARD

        if target_types is not None and link_type != WILDCARD:
            db_answer = self.db.get_matched_type_template(
                [link_type, *target_types]
            )
        elif targets is not None:
            db_answer = self.db.get_matched_links(link_type, targets)
        elif link_type != WILDCARD:
            db_answer = self.db.get_matched_type(link_type)
        else:
            # TODO: Improve this message error. What is invalid?
            raise ValueError("Invalid parameters")

        if output_format == QueryOutputFormat.HANDLE:
            return self._to_handle_list(db_answer)
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self._to_link_dict_list(db_answer)
        elif output_format == QueryOutputFormat.JSON:
            return self._to_json(db_answer)
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

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
        try:
            resp = self.db.get_link_type(link_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            logger().warning(
                f"An error occurred during the query. Detail:'{str(e)}'"
            )
            raise e

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
        try:
            resp = self.db.get_link_targets(link_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            logger().warning(
                f"An error occurred during the query. Detail:'{str(e)}'"
            )
            raise e

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
        try:
            resp = self.db.get_node_type(node_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            logger().warning(
                f"An error occurred during the query. Detail:'{str(e)}'"
            )
            raise e

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
        try:
            resp = self.db.get_node_name(node_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            logger().warning(
                f"An error occurred during the query. Detail:'{str(e)}'"
            )
            raise e

    def query(
        self,
        query: LogicalExpression,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> str:
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
            str: The result of the query in the specified output format.

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

            >>> result = obj.query(query=logical_expression)

            >>> print(result)
            {
                {'V1': '305e7d502a0ce80b94374ff0d79a6464', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'},
                {'V1': 'bd497eb24420dd50fed5f3d2e6cdd7c1', 'V2': '98870929d76a80c618e70a0393055b31', 'V3': '81ec21b0f1b03e18c55e056a56179fef'},
                {'V1': 'e2d9b15ab3461228d75502e754137caa', 'V2': 'c90242e2dbece101813762cc2a83d726', 'V3': '81ec21b0f1b03e18c55e056a56179fef'},
                ...
            }
        """
        query_answer = PatternMatchingAnswer()

        matched = query.matched(self.db, query_answer)

        if not matched:
            return ""

        tag_not = ""
        mapping = ""

        if query_answer.negation:
            tag_not = "NOT "

        if output_format == QueryOutputFormat.HANDLE:
            mapping = str(query_answer.assignments)
        elif output_format == QueryOutputFormat.ATOM_INFO:
            mapping = str(
                {
                    var: self.db.get_atom_as_dict(handle)
                    for var, handle in query_answer.assignments.items()
                }
            )
        elif output_format == QueryOutputFormat.JSON:
            mapping = json.dumps(
                {
                    var: self.db.get_atom_as_deep_representation(handle)
                    for var, handle in query_answer.assignments.items()
                },
                sort_keys=False,
                indent=4,
            )
        else:
            raise ValueError(f"Invalid output format: '{output_format}'")

        return f"{tag_not}{mapping}"

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        if self._db_type == DatabaseType.HASHTABLE.value:
            return self.db.add_node(node_params)
        else:
            raise MethodNotAllowed(
                message='This method is permited only in memory database',
                details='Instantiate the class sent the database type as `hash_table`',
            )

    def add_link(self, link_params: Dict[str, Any]) -> Dict[str, Any]:
        if self._db_type == DatabaseType.HASHTABLE.value:
            return self.db.add_link(link_params)
        else:
            raise MethodNotAllowed(
                message='This method is permited only in memory database',
                details='Instantiate the class sent the database type as `hash_table`',
            )
