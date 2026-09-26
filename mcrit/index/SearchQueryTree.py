from typing import List, Literal, Union

from mcrit.libs.tags import normalizeTag


##### Defining all Node types #####
class Node:
    pass


NodeType = Union["SearchTermNode", "SearchConditionNode", "NotNode", "AndNode", "OrNode"]


class _ListNode(Node):
    children: List[NodeType]
    _name = ""

    def __init__(self, children: List[NodeType], flatten=True):
        self.children = children
        if flatten:
            self.flatten()

    def flatten(self):
        flattened_children = []
        for child in self.children:
            if isinstance(child, type(self)):
                flattened_children.extend(child.children)
            else:
                flattened_children.append(child)
        self.children = flattened_children

    def __repr__(self) -> str:
        children_str = ", ".join([str(child) for child in self.children])
        return f"{self._name}({children_str})"


class AndNode(_ListNode):
    _name = "And"


class OrNode(_ListNode):
    _name = "Or"


class NotNode(Node):
    child: NodeType

    def __init__(self, child: NodeType):
        self.child = child

    def __repr__(self) -> str:
        return f"Not({str(self.child)})"


class SearchTermNode(Node):
    value: str

    def __init__(self, value):
        self.value = value

    def __repr__(self) -> str:
        return f"Search('{self.value}')"


class SearchConditionNode(Node):
    field: str
    operator: Literal["", "=", "<", "<=", ">", ">=", "!=", "?", "!?"]
    value: str

    def __init__(self, field, operator, value):
        self.field = field
        self.operator = operator
        self.value = value

    def __repr__(self) -> str:
        if self.operator != "":
            operator_str = self.operator
        else:
            operator_str = ":"
        return f"Condition('{self.field}' {operator_str} '{self.value}')"


#### Code operating on trees #####


class BaseVisitor:
    """
    BaseVisitor.visit traverses a given tree and returns a newly build identical tree.
    BaseVisitor is used as base class for more advanced tree transformations.
    """

    def visit(self, node: NodeType):
        if isinstance(node, SearchTermNode):
            return self.visitSearchTermNode(node)
        if isinstance(node, SearchConditionNode):
            return self.visitSearchConditionNode(node)
        if isinstance(node, AndNode):
            return self.visitAndNode(node)
        if isinstance(node, OrNode):
            return self.visitOrNode(node)
        if isinstance(node, NotNode):
            return self.visitNotNode(node)

    def visitOrNode(self, node: OrNode):
        visited_children = [self.visit(child) for child in node.children]
        return OrNode(visited_children)

    def visitAndNode(self, node: AndNode):
        visited_children = [self.visit(child) for child in node.children]
        return AndNode(visited_children)

    def visitNotNode(self, node: NotNode):
        return NotNode(self.visit(node.child))

    def visitSearchTermNode(self, node: SearchTermNode):
        return node

    def visitSearchConditionNode(self, node: SearchConditionNode):
        return node


class SearchFieldResolver(BaseVisitor):
    """
    SearchFieldResolver replaces "complex" SearchTermNodes with more simple SearchConditionNodes.
    This adds context information (i.e. in which fields do we want to search?).
    """

    def __init__(self, search_fields, conditional_search_fields=None) -> None:
        """
        search_fields: list of fields to search in
        conditional_search_fields: tuple (search_field, condition).
                                   Only search for node.value in search_field if condition(node.value) == True
        """
        super().__init__()
        self.search_fields = search_fields
        if conditional_search_fields is None:
            conditional_search_fields = []
        self.conditional_search_fields = conditional_search_fields

    # field names the query language accepts for another: "tag:packed" reads as naturally as
    # "tags:packed" and means the same (#53)
    FIELD_ALIASES = {"tag": "tags"}
    # fields whose stored values are normalised, and whose search values are normalised alike, so
    # that tags:Packed finds the tag stored as "packed" (see mcrit.libs.tags)
    NORMALISED_FIELDS = {"tags": normalizeTag}

    def visitSearchConditionNode(self, node: SearchConditionNode):
        field = self.FIELD_ALIASES.get(node.field, node.field)
        value = node.value
        if field in self.NORMALISED_FIELDS and isinstance(value, str):
            value = self.NORMALISED_FIELDS[field](value)
        if field == node.field and value == node.value:
            return node
        return SearchConditionNode(field, node.operator, value)

    def visitSearchTermNode(self, node: SearchTermNode):
        children = []
        for field in self.search_fields:
            children.append(SearchConditionNode(field, "?", node.value))
        for field, condition in self.conditional_search_fields:
            if condition(node.value):
                children.append(SearchConditionNode(field, "?", node.value))
        return OrNode(children)


class FilterSingleElementLists(BaseVisitor):
    """
    FilterSingleElementLists replaces And(node) as well as Or(node) by node.
    """

    def visitAndNode(self, node):
        children = [self.visit(child) for child in node.children]
        if len(children) == 1:
            return children[0]
        return AndNode(children)

    def visitOrNode(self, node):
        children = [self.visit(child) for child in node.children]
        if len(children) == 1:
            return children[0]
        return OrNode(children)


class Negate(BaseVisitor):
    """
    Negate negates a tree and uses De Morgans Law to move Not Nodes down the tree.
    Requires the tree to be free from SearchTermNodes (see SearchFieldResolver).
    The ouput is free from Not nodes.
    """

    def visitSearchTermNode(self, node: SearchTermNode):
        raise NotImplementedError

    def visitSearchConditionNode(self, node: SearchConditionNode):
        reverse_op_dict = {
            "": "!=",
            "=": "!=",
            "<": ">=",
            "<=": ">",
            ">": "<=",
            ">=": "<",
            "!=": "=",
            "?": "!?",
            "!?": "?",
        }
        return SearchConditionNode(node.field, reverse_op_dict[node.operator], node.value)

    def visitOrNode(self, node: OrNode):
        visited_children = [self.visit(child) for child in node.children]
        return AndNode(visited_children)

    def visitAndNode(self, node: AndNode):
        visited_children = [self.visit(child) for child in node.children]
        return OrNode(visited_children)

    def visitNotNode(self, node: NotNode):
        return PropagateNot().visit(node.child)


class PropagateNot(BaseVisitor):
    """
    PropagateNot uses De Morgans Law to move Not Nodes down the tree.
    Requires the tree to be free from SearchTermNodes (see SearchFieldResolver).
    The ouput is free from Not nodes.
    """

    def visitNotNode(self, node: NotNode):
        return Negate().visit(node.child)
