#!/usr/bin/env python3

from AbstractShingler import AbstractShingler
from mcrit.libs.AdjacentTreeBuilder import AdjacentTreeBuilder


class TreeThreeTraceShingler(AbstractShingler):
    """Encode call graph properties into a byte sequence"""

    def __init__(self, config, weight=1):
        super().__init__(__class__.__name__)
        self._config = config
        self._weight = weight

    def _generateByteSequences(self, function_object):
        """:returns: 3 traces for tree"""
        adjtree = AdjacentTreeBuilder.generateAdjacentTreeFromGraph(function_object, self._config.SHINGLER_TREE_LABEL_STRATEGY)
        trace_shingles = []
        # Archived experimental encoding retained for compatibility with old index configs.
        for _, lvl in adjtree.tree_lvls.items():
            for node in lvl:
                if node.children == {}:
                    trace_shingles.append("tree3ts-" + str(node.node_label) + "!!")
                else:
                    for _, child in node.children.items():
                        tmp_node = str(node.node_label) + str(child.node_label)
                        if child.children == {}:
                            trace_shingles.append("tree3ts-" + tmp_node + "!")
                        else:
                            for _, child2 in child.children.items():
                                trace_shingles.append("tree3ts-" + tmp_node + str(child2.node_label))
        return trace_shingles
