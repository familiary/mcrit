import unittest

from mcrit.index.SearchCursor import FullSearchCursor
from mcrit.index.SearchQueryTree import SearchConditionNode
from mcrit.storage.MongoDbStorage import MongoDbStorage, MongoSearchTranspiler


class TestMongoSearchTranspiler(unittest.TestCase):
    def _visit(self, field, operator, value, padded=False):
        return MongoSearchTranspiler(pichash_padded=padded).visit(SearchConditionNode(field, operator, value))

    def test_pichash_equality_on_a_padded_instance_is_encoded_zero_padded(self):
        # pichashes are stored hex-encoded in the "_pichash" field, 16 digits wide once migrated (#145)
        self.assertEqual(self._visit("pichash", "=", "0x1234", padded=True), {"_pichash": "0x0000000000001234"})
        self.assertEqual(self._visit("pichash", "!=", "0x1234", padded=True), {"_pichash": {"$ne": "0x0000000000001234"}})
        self.assertEqual(self._visit("pichash", "=", "4660", padded=True), {"_pichash": "0x0000000000001234"})

    def test_pichash_equality_on_an_unpadded_instance_accepts_both_widths(self):
        # a migration in flight leaves both shapes in the collection, so neither may be missed
        self.assertEqual(self._visit("pichash", "=", "0x1234"), {"_pichash": {"$in": ["0x0000000000001234", "0x1234"]}})
        self.assertEqual(self._visit("pichash", "!=", "0x1234"), {"_pichash": {"$nin": ["0x0000000000001234", "0x1234"]}})
        self.assertEqual(self._visit("pichash", "=", "4660"), {"_pichash": {"$in": ["0x0000000000001234", "0x1234"]}})

    def test_pichash_range_operators_need_a_padded_instance(self):
        for operator in ("<", "<=", ">", ">="):
            with self.assertRaises(ValueError):
                self._visit("pichash", operator, "0x1234")
        self.assertEqual(self._visit("pichash", "<", "0x99", padded=True), {"_pichash": {"$lt": "0x0000000000000099"}})
        self.assertEqual(self._visit("pichash", ">=", "0x99", padded=True), {"_pichash": {"$gte": "0x0000000000000099"}})

    def test_pichash_bounds_outside_64_bits_are_rejected(self):
        # a wider bound would format to more than 16 digits and not compare lexicographically
        for value in ("0x10000000000000000", "-1"):
            with self.assertRaises(ValueError):
                self._visit("pichash", "<", value, padded=True)
        self.assertEqual({"_pichash": {"$lte": "0xffffffffffffffff"}}, self._visit("pichash", "<=", "0xffffffffffffffff", padded=True))

    def test_padded_hex_orders_like_the_numbers(self):
        values = [0x99, 0x1000, 0x4D2, 0xFFFFFFFFFFFFFFFF, 0]
        encoded = [self._visit("pichash", "=", str(v), padded=True)["_pichash"] for v in values]
        self.assertEqual(sorted(encoded), [self._visit("pichash", "=", str(v), padded=True)["_pichash"] for v in sorted(values)])

    def test_pichash_substring_search_matches_the_encoded_field(self):
        # "?" produces a regex, which cannot be hex-encoded (and used to raise a TypeError here),
        # so it is matched against the stored representation and only the field gets renamed
        condition = self._visit("pichash", "?", "1234")
        self.assertEqual(["_pichash"], list(condition))
        self.assertTrue(condition["_pichash"].search("0x1234"))
        self.assertTrue(condition["_pichash"].search("0x0000000000001234"))
        self.assertIsNone(condition["_pichash"].search("0x5678"))

    def test_pichash_negated_substring_search_matches_the_encoded_field(self):
        condition = self._visit("pichash", "!?", "1234")
        self.assertEqual(["_pichash"], list(condition))
        self.assertEqual(["$not"], list(condition["_pichash"]))
        self.assertTrue(condition["_pichash"]["$not"].search("0x1234"))

    def test_other_fields_are_untouched(self):
        self.assertEqual(self._visit("num_functions", ">", "5"), {"num_functions": {"$gt": 5}})
        self.assertEqual(self._visit("family", "=", "somefamily"), {"family": "somefamily"})


class _StorageWithPadding(MongoDbStorage):
    """The sort helpers without a database: only the padding flag is consulted."""

    def __init__(self, padded):  # noqa: D107 - deliberately skips MongoDbStorage.__init__
        self._padded = padded

    def isPichashPadded(self):
        return self._padded


class TestMongoSortableFields(unittest.TestCase):
    def _cursor(self, *sort_fields):
        return FullSearchCursor(None, [(field, True) for field in sort_fields])

    def test_pichash_cannot_be_sorted_by_on_an_unpadded_instance(self):
        # the cursor pages by comparing the sort field, which variable-width hex does not allow
        storage = _StorageWithPadding(False)
        with self.assertRaises(ValueError):
            storage._assert_sortable_fields(self._cursor("pichash", "function_id"))
        storage._assert_sortable_fields(None)
        storage._assert_sortable_fields(self._cursor("function_id"))
        storage._assert_sortable_fields(self._cursor("function_name", "function_id"))

    def test_pichash_sorts_by_the_stored_field_on_a_padded_instance(self):
        storage = _StorageWithPadding(True)
        self.assertEqual([("_pichash", 1), ("function_id", 1)], storage._get_sort_list_from_cursor(self._cursor("pichash", "function_id")))
        self.assertIsNone(storage._get_sort_list_from_cursor(None))


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
