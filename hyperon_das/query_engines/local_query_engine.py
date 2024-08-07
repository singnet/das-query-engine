import re
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

from hyperon_das_atomdb import WILDCARD, AtomDB
from hyperon_das_atomdb.adapters import InMemoryDB
from hyperon_das_atomdb.exceptions import AtomDoesNotExist

from hyperon_das.cache.iterators import (
    AndEvaluator,
    CustomQuery,
    LazyQueryEvaluator,
    ListIterator,
    LocalGetLinks,
    LocalIncomingLinks,
    QueryAnswerIterator,
)
from hyperon_das.client import FunctionsClient
from hyperon_das.context import Context
from hyperon_das.exceptions import UnexpectedQueryFormat
from hyperon_das.logger import logger
from hyperon_das.query_engines.query_engine_protocol import QueryEngine
from hyperon_das.type_alias import Query
from hyperon_das.utils import Assignment, QueryAnswer, das_error


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
            except AtomDoesNotExist:
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
            return LazyQueryEvaluator(
                link_type=query["type"],
                source=matched_targets,
                query_engine=self,
                query_parameters=parameters,
            )
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
            except AtomDoesNotExist:
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
                AtomDoesNotExist(message="Nonexistent atom", details=f'{node_type}:{node_name}')
            )

    def get_link(self, link_type: str, link_targets: List[str]) -> Union[Dict[str, Any], None]:
        try:
            link_handle = self.local_backend.link_handle(link_type, link_targets)
            return self.local_backend.get_atom(link_handle)
        except AtomDoesNotExist:
            das_error(
                AtomDoesNotExist(message='Nonexistent atom', details=f'{link_type}:{link_targets}')
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

    def count_atoms(self, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        if parameters and parameters.get('context') == 'remote':
            return {}
        return self.local_backend.count_atoms(parameters)

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
