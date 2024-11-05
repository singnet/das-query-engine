"""
Example usage of DictQueryTokenizer:

Sample query dictionary:
    >>> sample_query = {
    >>>     "atom_type": "link",
    >>>     "type": "Expression",
    >>>     "targets": [
    >>>         {"atom_type": "node", "type": "Symbol", "name": "Similarity"},
    >>>         {
    >>>             "atom_type": "link",
    >>>             "type": "Expression",
    >>>             "targets": [
    >>>                 {"atom_type": "node", "type": "Symbol", "name": "Concept"},
    >>>                 {"atom_type": "node", "type": "Symbol", "name": '"human"'},
    >>>             ],
    >>>         },
    >>>         {"atom_type": "variable", "name": "v1"},
    >>>     ],
    >>> }

Tokenizing the sample query:
    >>> print(DictQueryTokenizer.tokenize(sample_query))
    Output:
    LINK_TEMPLATE Expression 3 NODE Symbol Similarity LINK Expression 2 NODE Symbol Concept NODE Symbol "human" VARIABLE v1

Sample tokenized string:
    >>> sample_tokens = '''
    >>>     LINK_TEMPLATE Expression 3
    >>>         NODE Symbol Similarity
    >>>         LINK Expression 2
    >>>             NODE Symbol Concept
    >>>             NODE Symbol "human"
    >>>         VARIABLE v1
    >>> '''

Untokenizing the sample tokens:
    >>> print(DictQueryTokenizer.untokenize(sample_tokens))
    Output:
    {
      'atom_type': 'link',
      'type': 'Expression',
      'targets': [
        {'atom_type': 'node', 'type': 'Symbol', 'name': 'Similarity'},
        {
          'atom_type': 'link',
          'type': 'Expression',
          'targets': [
            {'atom_type': 'node', 'type': 'Symbol', 'name': 'Concept'},
            {'atom_type': 'node', 'type': 'Symbol', 'name': '"human"'}
          ]
        },
        {'atom_type': 'variable', 'name': 'v1'}
      ]
    }
"""

import dataclasses
from typing import Any, Callable, Type, TypeAlias

TOKENS_DELIMITER = " "

FromTokensDictKey: TypeAlias = str
FromTokensDictValue: TypeAlias = Callable[[list[str], int], tuple[int, Any]]
FROM_TOKENS_DICT: dict[FromTokensDictKey, FromTokensDictValue] = {}
"""A mapping of tokens tags to functions that create instances of the corresponding types from tokens."""


@dataclasses.dataclass
class Node:
    """
    A class representing a node in the tokenizer.

    Attributes:
        type (str): The type of the node.
        name (str): The name of the node.
    """

    type: str
    name: str

    def to_tokens(self) -> list[str]:
        """
        Convert the Node to a list of tokens.

        Returns:
            list[str]: A list of string tokens representing the Node.
        """
        return ["NODE", self.type, self.name]

    @staticmethod
    def from_tokens(tokens: list[str], cursor: int = 0) -> tuple[int, "Node"]:
        """
        Create a Node instance from a list of tokens.

        Args:
            tokens (list[str]): The list of tokens to parse.
            cursor (int, optional): The starting position in the token list. Defaults to 0.

        Returns:
            tuple[int, Node]: A tuple containing the updated cursor position and the created Node instance.

        Raises:
            ValueError: If the tokens do not represent a valid Node.
        """
        if tokens[cursor] == "NODE":
            cursor += 1  # Skip the "NODE" token
            node = Node(type=tokens[cursor], name=tokens[cursor + 1])
            cursor += 2  # Skip the type and name tokens
            return cursor, node
        raise ValueError(f"Unsupported sequence of tokens: {tokens[cursor:]}")


FROM_TOKENS_DICT["NODE"] = Node.from_tokens


@dataclasses.dataclass
class Variable:
    """
    A class representing a variable in the tokenizer.

    Attributes:
        name (str): The name of the variable.
    """

    name: str

    def to_tokens(self) -> list[str]:
        """
        Convert the variable to a list of tokens.

        Returns:
            list[str]: A list of string tokens representing the variable.
        """
        return ["VARIABLE", self.name]

    @staticmethod
    def from_tokens(tokens: list[str], cursor: int = 0) -> tuple[int, "Variable"]:
        """
        Create a Variable instance from a list of tokens.

        Args:
            tokens (list[str]): The list of tokens to parse.
            cursor (int, optional): The starting position in the token list. Defaults to 0.

        Returns:
            tuple[int, Variable]: A tuple containing the updated cursor position and the created Variable instance.

        Raises:
            ValueError: If the tokens do not represent a valid Variable.
        """
        if tokens[cursor] == "VARIABLE":
            cursor += 1  # Skip the "VARIABLE" token
            variable = Variable(name=tokens[cursor])
            cursor += 1  # Skip the name token
            return cursor, variable
        raise ValueError(f"Unsupported sequence of tokens: {tokens[cursor:]}")


FROM_TOKENS_DICT["VARIABLE"] = Variable.from_tokens


@dataclasses.dataclass
class Link:
    """
    A class representing a link in the tokenizer.

    Attributes:
        type (str): The type of the link.
        targets (list[Link | Node | Variable]): A list of targets associated with the link.
        is_template (bool): Indicates if the link is a template. Defaults to False.
    """

    type: str
    targets: list[Any] = dataclasses.field(default_factory=list)
    is_template: bool = False

    def to_tokens(self) -> list[str]:
        """
        Convert the link to a list of tokens.

        Returns:
            list[str]: A list of string tokens representing the link.
        """
        return [
            "LINK_TEMPLATE" if self.is_template else "LINK",
            self.type,
            str(len(self.targets)),
            *[token for target in self.targets for token in target.to_tokens()],
        ]

    @staticmethod
    def from_tokens(tokens: list[str], cursor: int = 0) -> tuple[int, "Link"]:
        """
        Create a Link instance from a list of tokens.

        Args:
            tokens (list[str]): The list of tokens to parse.
            cursor (int, optional): The starting position in the token list. Defaults to 0.

        Returns:
            tuple[int, Link]: A tuple containing the updated cursor position and the created Link instance.

        Raises:
            ValueError: If the tokens do not represent a valid Link.
        """
        if tokens[cursor] in {"LINK", "LINK_TEMPLATE"}:
            link_tag = tokens[cursor]
            cursor += 1  # Skip the "LINK" or "LINK_TEMPLATE" token
            link = Link(type=tokens[cursor])
            cursor += 1  # Skip the type token
            target_count = int(tokens[cursor])
            cursor += 1  # Skip the target count token
            for _ in range(target_count):
                try:
                    cursor, target = FROM_TOKENS_DICT[tokens[cursor]](tokens, cursor)
                    if isinstance(target, Variable):
                        link.is_template = True
                    link.targets.append(target)
                except KeyError as ex:
                    raise ValueError(f"Unsupported token: {tokens[cursor:]}, key error: {ex}")
            if link_tag == "LINK_TEMPLATE" and not link.is_template:
                raise ValueError("Template link without variables")
            elif link_tag == "LINK" and link.is_template:
                raise ValueError("Non-template link with variables")
            return cursor, link
        raise ValueError(f"Unsupported sequence of tokens: {tokens[cursor:]}")


FROM_TOKENS_DICT["LINK"] = FROM_TOKENS_DICT["LINK_TEMPLATE"] = Link.from_tokens


@dataclasses.dataclass
class OrOperator:
    """
    A class representing an OR operator in the tokenizer.

    Attributes:
        operands (list[Link]): A list of operands associated with the OR operator.
    """

    operands: list[Link] = dataclasses.field(default_factory=list)

    def to_tokens(self) -> list[str]:
        """
        Convert the OR operator to a list of tokens.

        Returns:
            list[str]: A list of string tokens representing the OR operator.
        """
        return [
            "OR",
            str(len(self.operands)),
            *[token for operand in self.operands for token in operand.to_tokens()],
        ]

    @staticmethod
    def from_tokens(tokens: list[str], cursor: int = 0) -> tuple[int, "OrOperator"]:
        """
        Create an OrOperator instance from a list of tokens.

        Args:
            tokens (list[str]): The list of tokens to parse.
            cursor (int, optional): The starting position in the token list. Defaults to 0.

        Returns:
            tuple[int, OrOperator]: A tuple containing the updated cursor position and the created
                                    OrOperator instance.

        Raises:
            ValueError: If the tokens do not represent a valid OrOperator.
        """
        if tokens[cursor] == "OR":
            cursor += 1  # Skip the "OR" token
            operator = OrOperator()
            operand_count = int(tokens[cursor])
            cursor += 1  # Skip the operand count token
            for _ in range(operand_count):
                cursor, operand = FROM_TOKENS_DICT[tokens[cursor]](tokens, cursor)
                operator.operands.append(operand)
            return cursor, operator
        raise ValueError(f"Unsupported sequence of tokens: {tokens[cursor-1:]}")


FROM_TOKENS_DICT["OR"] = OrOperator.from_tokens


@dataclasses.dataclass
class AndOperator:
    """
    A class representing an AND operator in the tokenizer.

    Attributes:
        operands (list[Link]): A list of operands associated with the AND operator.
    """

    operands: list[Link] = dataclasses.field(default_factory=list)

    def to_tokens(self) -> list[str]:
        """
        Convert the AND operator to a list of tokens.

        Returns:
            list[str]: A list of string tokens representing the AND operator.
        """
        return [
            "AND",
            str(len(self.operands)),
            *[token for operand in self.operands for token in operand.to_tokens()],
        ]

    @staticmethod
    def from_tokens(tokens: list[str], cursor: int = 0) -> tuple[int, "AndOperator"]:
        """
        Create an AndOperator instance from a list of tokens.

        Args:
            tokens (list[str]): The list of tokens to parse.
            cursor (int, optional): The starting position in the token list. Defaults to 0.

        Returns:
            tuple[int, AndOperator]: A tuple containing the updated cursor position and the created
                                     AndOperator instance.

        Raises:
            ValueError: If the tokens do not represent a valid AndOperator.
        """
        if tokens[cursor] == "AND":
            cursor += 1
            operator = AndOperator()
            operand_count = int(tokens[cursor])
            cursor += 1
            for _ in range(operand_count):
                cursor, operand = FROM_TOKENS_DICT[tokens[cursor]](tokens, cursor)
                operator.operands.append(operand)
            return cursor, operator
        raise ValueError(f"Unsupported sequence of tokens: {tokens[cursor:]}")


FROM_TOKENS_DICT["AND"] = AndOperator.from_tokens


@dataclasses.dataclass
class NotOperator:
    """
    A class representing a NOT operator in the tokenizer.

    Attributes:
        operand (Link): The operand associated with the NOT operator.
    """

    operand: Link

    def to_tokens(self) -> list[str]:
        """
        Convert the NOT operator to a list of tokens.

        Returns:
            list[str]: A list of string tokens representing the NOT operator.
        """
        return ["NOT", *self.operand.to_tokens()]

    @staticmethod
    def from_tokens(tokens: list[str], cursor: int = 0) -> tuple[int, "NotOperator"]:
        """
        Create a NotOperator instance from a list of tokens.

        Args:
            tokens (list[str]): The list of tokens to parse.
            cursor (int, optional): The starting position in the token list. Defaults to 0.

        Returns:
            tuple[int, NotOperator]: A tuple containing the updated cursor position and the created
                                     NotOperator instance.

        Raises:
            ValueError: If the tokens do not represent a valid NotOperator.
        """
        if tokens[cursor] == "NOT":
            cursor += 1
            cursor, operand = FROM_TOKENS_DICT[tokens[cursor]](tokens, cursor)
            return cursor, NotOperator(operand)
        raise ValueError(f"Unsupported sequence of tokens: {tokens[cursor:]}")


FROM_TOKENS_DICT["NOT"] = NotOperator.from_tokens


class DictQueryTokenizer:
    """A class for tokenizing dictionary queries."""

    Query: TypeAlias = dict[str, Any]  # type alias for a dictionary representing a query

    ToQueryDictKey: TypeAlias = Type[
        Node | Variable | Link | OrOperator | AndOperator | NotOperator
    ]
    ToQueryDictValue: TypeAlias = Callable[
        [Node | Variable | Link | OrOperator | AndOperator | NotOperator],
        Query,
    ]
    to_query_dict: dict[ToQueryDictKey, ToQueryDictValue] = {
        # A mapping of types to functions that convert instances of those types to query dictionaries.
        Node: lambda node: {
            "atom_type": "node",
            "type": node.type,
            "name": node.name,
        },
        Variable: lambda variable: {
            "atom_type": "variable",
            "name": variable.name,
        },
        Link: lambda link: {
            "atom_type": "link",
            "type": link.type,
            "targets": [
                DictQueryTokenizer.to_query_dict[type(target)](target) for target in link.targets
            ],
        },
        OrOperator: lambda operator: {
            "or": [
                DictQueryTokenizer.to_query_dict[type(operand)](operand)
                for operand in operator.operands
            ],
        },
        AndOperator: lambda operator: {
            "and": [
                DictQueryTokenizer.to_query_dict[type(operand)](operand)
                for operand in operator.operands
            ],
        },
        NotOperator: lambda operator: {
            "not": DictQueryTokenizer.to_query_dict[type(operator.operand)](operator.operand),
        },
    }

    @staticmethod
    def tokenize(query: Query) -> str:
        """
        Convert a query dictionary into a tokenized string.

        Args:
            query (Query): The query dictionary to tokenize.

        Returns:
            str: A tokenized string representation of the query.

        Raises:
            ValueError: If the query cannot be tokenized.
        """

        def _tokenize(
            _query: DictQueryTokenizer.Query, _parent: Link | None = None
        ) -> Link | Node | Variable | OrOperator | AndOperator:
            match _query:
                case {"or": list()}:
                    return OrOperator(operands=[_tokenize(o) for o in _query["or"]])
                case {"and": list()}:
                    return AndOperator(operands=[_tokenize(o) for o in _query["and"]])
                case {"not": dict()}:
                    return NotOperator(_tokenize(_query["not"]))
                case {"atom_type": "link", "type": str(), "targets": list()}:
                    link = Link(_query["type"])
                    link.targets += [_tokenize(t, _parent=link) for t in _query["targets"]]
                    return link
                case {"atom_type": "node", "type": str(), "name": str()}:
                    return Node(_query["type"], _query["name"])
                case {"atom_type": "variable", "name": str()}:
                    _parent.is_template = True
                    return Variable(_query["name"])
                case _:
                    raise ValueError(f"Unsupported query: {_query}")

        return TOKENS_DELIMITER.join(_tokenize(query).to_tokens())

    @staticmethod
    def untokenize(query_tokens: str) -> Query:
        """
        Convert a tokenized string back into a query dictionary.

        Args:
            query_tokens (str): The tokenized string to untokenize.

        Returns:
            Query: A dictionary representation of the query.

        Raises:
            ValueError: If the tokens cannot be untokenized into a valid query.
        """
        tokens = query_tokens.split()
        cursor = 0
        try:
            cursor, query = FROM_TOKENS_DICT[tokens[cursor]](tokens, cursor)
            return DictQueryTokenizer.to_query_dict[type(query)](query)
        except KeyError as ex:
            raise ValueError(f"Unsupported element: {tokens[cursor:]}, key error: {ex}")

# EXAMPLES OF USE:
#
# sample_query = {
#     "and": [
#         {
#             "not": {
#                 "atom_type": "link",
#                 "type": "Expression",
#                 "targets": [
#                     {"atom_type": "node", "type": "Symbol", "name": "Similarity"},
#                     {
#                         "atom_type": "link",
#                         "type": "Expression",
#                         "targets": [
#                             {"atom_type": "node", "type": "Symbol", "name": "Concept"},
#                             {"atom_type": "node", "type": "Symbol", "name": '"human"'},
#                         ],
#                     },
#                     {"atom_type": "variable", "name": "v1"},
#                 ],
#             },
#         },
#         {
#             'atom_type': 'link',
#             'type': 'Expression',
#             'targets': [
#                 {'not': {'atom_type': 'node', 'type': 'Symbol', 'name': 'Similarity'}},
#                 {
#                     'atom_type': 'link',
#                     'type': 'Expression',
#                     'targets': [
#                         {'atom_type': 'node', 'type': 'Symbol', 'name': 'Concept'},
#                         {'atom_type': 'node', 'type': 'Symbol', 'name': '"human"'},
#                     ],
#                 },
#                 {'atom_type': 'variable', 'name': 'v1'},
#             ],
#         },
#     ],
# }

# print(DictQueryTokenizer.tokenize(sample_query))
# """
# Output:
# LINK_TEMPLATE Expression 3 NODE Symbol Similarity LINK Expression 2 NODE Symbol Concept NODE Symbol "human" VARIABLE v1
# """

# sample_tokens = """
#     LINK_TEMPLATE Expression 3
#         NOT NODE Symbol Similarity
#         LINK Expression 2
#             NODE Symbol Concept
#             NODE Symbol "human"
#         VARIABLE v1
# """
# sample_tokens = """AND 2 NOT LINK_TEMPLATE Expression 3 NODE Symbol Similarity LINK Expression 2 NODE Symbol Concept NODE Symbol "human" VARIABLE v1 LINK_TEMPLATE Expression 3 NOT NODE Symbol Similarity LINK Expression 2 NODE Symbol Concept NODE Symbol "human" VARIABLE v1"""

# print(DictQueryTokenizer.untokenize(sample_tokens))
# """
# Output:
# {
#   'atom_type': 'link',
#   'type': 'Expression',
#   'targets': [
#     {'atom_type': 'node', 'type': 'Symbol', 'name': 'Similarity'},
#     {
#       'atom_type': 'link',
#       'type': 'Expression',
#       'targets': [
#         {'atom_type': 'node', 'type': 'Symbol', 'name': 'Concept'},
#         {'atom_type': 'node', 'type': 'Symbol', 'name': '"human"'}
#       ]
#     },
#     {'atom_type': 'variable', 'name': 'v1'}
#   ]
# }
# """
