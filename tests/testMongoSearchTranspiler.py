import unittest

from mcrit.index.SearchCursor import FullSearchCursor
from mcrit.index.SearchQueryTree import SearchConditionNode
from mcrit.storage.MongoDbStorage import MongoDbStorage, MongoSearchTranspiler


class TestMongoSearchTranspiler(unittest.TestCase):
    def _visit(self, field, operator, value):
        return MongoSearchTranspiler().visit(SearchConditionNode(field, operator, value))

    def test_pichash_equality_is_encoded(self):
        # pichashes are stored hex-encoded in the "_pichash" field
        self.assertEqual(self._visit("pichash", "=", "0x1234"), {"_pichash": "0x1234"})

    def test_pichash_inequality_is_encoded(self):
        self.assertEqual(self._visit("pichash", "!=", "0x1234"), {"_pichash": {"$ne": "0x1234"}})

    def test_pichash_decimal_value_is_encoded_as_hex(self):
        self.assertEqual(self._visit("pichash", "=", "4660"), {"_pichash": "0x1234"})

    def test_pichash_range_operators_are_rejected(self):
        for operator in ("<", "<=", ">", ">="):
            with self.assertRaises(ValueError):
                self._visit("pichash", operator, "0x1234")

    def test_pichash_substring_search_matches_the_encoded_field(self):
        # "?" produces a regex, which cannot be hex-encoded (and used to raise a TypeError here),
        # so it is matched against the stored representation and only the field gets renamed
        condition = self._visit("pichash", "?", "1234")
        self.assertEqual(["_pichash"], list(condition))
        self.assertTrue(condition["_pichash"].search("0x1234"))
        self.assertIsNone(condition["_pichash"].search("0x5678"))

    def test_pichash_negated_substring_search_matches_the_encoded_field(self):
        condition = self._visit("pichash", "!?", "1234")
        self.assertEqual(["_pichash"], list(condition))
        self.assertEqual(["$not"], list(condition["_pichash"]))
        self.assertTrue(condition["_pichash"]["$not"].search("0x1234"))

    def test_other_fields_are_untouched(self):
        self.assertEqual(self._visit("num_functions", ">", "5"), {"num_functions": {"$gt": 5}})
        self.assertEqual(self._visit("family", "=", "somefamily"), {"family": "somefamily"})


class TestMongoSortableFields(unittest.TestCase):
    def _cursor(self, *sort_fields):
        return FullSearchCursor(None, [(field, True) for field in sort_fields])

    def test_pichash_cannot_be_sorted_by(self):
        # the cursor pages by comparing the sort field, which the hex-encoded pichash does not allow
        with self.assertRaises(ValueError):
            MongoDbStorage._assert_sortable_fields(self._cursor("pichash", "function_id"))

    def test_regular_fields_can_be_sorted_by(self):
        MongoDbStorage._assert_sortable_fields(None)
        MongoDbStorage._assert_sortable_fields(self._cursor("function_id"))
        MongoDbStorage._assert_sortable_fields(self._cursor("function_name", "function_id"))


if __name__ == "__main__":
    unittest.main()


class TestSubstringSearchOverKnownValues(unittest.TestCase):
    """A substring condition on a field whose distinct values are known is answered as an
    $in / $nin of the matching values, so MongoDB uses the index instead of scanning every
    document with a regex (fkie-cad/mcritweb#76)."""

    NAMES = ["", "main", "WinMain", "decrypt_config", "sub_401000", None]

    def _visit(self, operator, value, field="function_name"):
        return MongoSearchTranspiler({"function_name": self.NAMES}).visit(SearchConditionNode(field, operator, value))

    def test_a_substring_condition_becomes_the_matching_values(self):
        self.assertEqual({"function_name": {"$in": ["main", "WinMain"]}}, self._visit("?", "main"))

    def test_the_match_is_case_insensitive_like_the_regex(self):
        self.assertEqual({"function_name": {"$in": ["main", "WinMain"]}}, self._visit("?", "MAIN"))
        self.assertEqual({"function_name": {"$in": ["decrypt_config"]}}, self._visit("?", "CRYPT_"))

    def test_nothing_matching_is_an_empty_list(self):
        # matches no document at all, without MongoDB looking at a single one
        self.assertEqual({"function_name": {"$in": []}}, self._visit("?", "zzzzq"))

    def test_the_negated_condition_excludes_the_matching_values(self):
        self.assertEqual({"function_name": {"$nin": ["main", "WinMain"]}}, self._visit("!?", "main"))

    def test_regex_metacharacters_are_literal(self):
        self.assertEqual({"function_name": {"$in": []}}, self._visit("?", "sub_.*"))
        self.assertEqual({"function_name": {"$in": ["sub_401000"]}}, self._visit("?", "sub_4"))

    def test_an_empty_term_keeps_the_regex(self):
        condition = self._visit("?", "")
        self.assertEqual(["function_name"], list(condition))
        self.assertTrue(hasattr(condition["function_name"], "search"))

    def test_other_fields_and_operators_keep_their_form(self):
        self.assertEqual({"function_name": "main"}, self._visit("=", "main"))
        condition = self._visit("?", "main", field="family")
        self.assertTrue(hasattr(condition["family"], "search"))


class TestFoldOtherLabels(unittest.TestCase):
    """The tree a plain term gives, as it holds for a function without other labels (#301)."""

    @staticmethod
    def _fold(term):
        from mcrit.index.SearchQueryParser import SearchQueryParser
        from mcrit.index.SearchQueryTree import FilterSingleElementLists, PropagateNot, SearchFieldResolver
        from mcrit.storage.MongoDbStorage import _foldOtherLabels

        tree = SearchFieldResolver(MongoDbStorage._PLAIN_FUNCTION_SEARCH_FIELDS).visit(SearchQueryParser().parse(term))
        return repr(_foldOtherLabels(PropagateNot().visit(FilterSingleElementLists().visit(tree))))

    def test_a_plain_term_is_the_name_alone(self):
        self.assertEqual("Condition('function_name' ? 'abc')", self._fold("abc"))
        self.assertEqual("Condition('function_name' !? 'abc')", self._fold("NOT abc"))
        self.assertEqual("And(Condition('function_name' ? 'abc'), Condition('sample_id' : '3'))", self._fold("abc sample_id:3"))
        self.assertEqual("Or(Condition('function_name' ? 'a'), Condition('function_name' ? 'b'))", self._fold("a OR b"))

    def test_conditions_on_the_field_alone_fold_into_constants(self):
        self.assertEqual("False", self._fold("_other_labels:?abc"))
        self.assertEqual("True", self._fold("_other_labels:!?abc"))
        self.assertEqual("False", self._fold("_other_labels:?abc sample_id:3"))
        self.assertEqual("Condition('sample_id' : '3')", self._fold("_other_labels:!?abc sample_id:3"))
        self.assertEqual("True", self._fold("_other_labels:?abc OR NOT _other_labels:?x"))
