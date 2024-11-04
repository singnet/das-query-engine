import dataclasses
from typing import Any, TypeAlias

TOKENS_DELIMITER = " "


@dataclasses.dataclass
class Node:
    type: str
    name: str

    def to_tokens(self) -> list[str]:
        return ["NODE", self.type, self.name]

    @staticmethod
    def from_tokens(tokens: list[str], cursor: int = 0) -> tuple[int, "Node"]:
        match tokens[cursor]:
            case "NODE":
                cursor += 1  # Skip the "NODE" token
                node = Node(type=tokens[cursor], name=tokens[cursor + 1])
                cursor += 2  # Skip the type and name tokens
                return cursor, node
            case _:
                raise ValueError(f"Unsupported token: {tokens[cursor:]}")


@dataclasses.dataclass
class Variable:
    name: str

    def to_tokens(self) -> list[str]:
        return ["VARIABLE", self.name]

    @staticmethod
    def from_tokens(tokens: list[str], cursor: int = 0) -> tuple[int, "Variable"]:
        match tokens[cursor]:
            case "VARIABLE":
                cursor += 1  # Skip the "VARIABLE" token
                variable = Variable(name=tokens[cursor])
                cursor += 1  # Skip the name token
                return cursor, variable
            case _:
                raise ValueError(f"Unsupported token: {tokens[cursor:]}")


@dataclasses.dataclass
class Link:
    type: str
    targets: list[Any] = dataclasses.field(default_factory=list)
    is_template: bool = False

    def to_tokens(self) -> list[str]:
        return [
            "LINK_TEMPLATE" if self.is_template else "LINK",
            self.type,
            str(len(self.targets)),
            *[token for target in self.targets for token in target.to_tokens()],
        ]

    @staticmethod
    def from_tokens(tokens: list[str], cursor: int = 0) -> tuple[int, "Link"]:
        match tokens[cursor]:
            case "LINK" | "LINK_TEMPLATE":
                link_tag = tokens[cursor]
                cursor += 1  # Skip the "LINK" or "LINK_TEMPLATE" token
                link = Link(type=tokens[cursor])
                cursor += 1  # Skip the type token
                target_count = int(tokens[cursor])
                cursor += 1  # Skip the target count token
                for _ in range(target_count):
                    match tokens[cursor]:
                        case "NODE":
                            cursor, target = Node.from_tokens(tokens, cursor)
                        case "VARIABLE":
                            link.is_template = True
                            cursor, target = Variable.from_tokens(tokens, cursor)
                        case "LINK" | "LINK_TEMPLATE":
                            cursor, target = Link.from_tokens(tokens, cursor)
                        case _:
                            raise ValueError(f"Unsupported token: {tokens[cursor:]}")
                    link.targets.append(target)

                if link_tag == "LINK_TEMPLATE" and not link.is_template:
                    raise ValueError("Template link without variables")
                elif link_tag == "LINK" and link.is_template:
                    raise ValueError("Non-template link with variables")

                return cursor, link
            case _:
                raise ValueError(f"Unsupported token: {tokens[cursor:]}")


class DictQueryTokenizer:
    Query: TypeAlias = dict[str, Any]

    @staticmethod
    def tokenize(_query: Query) -> str:
        def _tokenize(
            _query: DictQueryTokenizer.Query, _parent: Link | None = None
        ) -> Link | Node | Variable:
            match _query:
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

        return TOKENS_DELIMITER.join(_tokenize(_query).to_tokens())

    @staticmethod
    def untokenize(tokens: str) -> Query:
        _, link = Link.from_tokens(tokens.split())

        def _untokenize(_atom: Link | Node | Variable) -> DictQueryTokenizer.Query:
            match _atom:
                case Link(type, targets):
                    return {
                        "atom_type": "link",
                        "type": type,
                        "targets": [_untokenize(target) for target in targets],
                    }
                case Node(type, name):
                    return {"atom_type": "node", "type": type, "name": name}
                case Variable(name):
                    return {"atom_type": "variable", "name": name}
                case _:
                    raise ValueError(f"Unsupported element: {_atom}")

        return _untokenize(link)


sample_query = {
    "atom_type": "link",
    "type": "Expression",
    "targets": [
        {"atom_type": "node", "type": "Symbol", "name": "Similarity"},
        {
            "atom_type": "link",
            "type": "Expression",
            "targets": [
                {"atom_type": "node", "type": "Symbol", "name": "Concept"},
                {"atom_type": "node", "type": "Symbol", "name": '"human"'},
            ],
        },
        {"atom_type": "variable", "name": "v1"},
    ],
}

print(DictQueryTokenizer.tokenize(sample_query))

sample_tokens = """
    LINK_TEMPLATE Expression 3
        NODE Symbol Similarity
        LINK Expression 2
            NODE Symbol Concept
            NODE Symbol "human"
        VARIABLE v1
"""

print(DictQueryTokenizer.untokenize(sample_tokens))
