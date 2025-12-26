from unittest import TestCase
from ..utils import (
    format_sql_rows,
    generate_export_sql,
    validate_sql_with_sqlglot,
    generate_unique_query_key,
    clean_sql_input,
    _compute_query_depth,
)
import sqlglot
from django.core.cache import cache
from unittest.mock import patch
from sqlglot.expressions import Expression


class UtilsTestCase(TestCase):

    def test_format_sql_rows(self):
        result = [
            {"id": 1, "name": "Alice", "age": None},
            {"id": 2, "name": "Bob", "age": 30}
        ]
        columns = ["id", "name", "age"]
        expected = [
            "  (1, 'Alice', NULL)",
            "  (2, 'Bob', 30)"
        ]
        self.assertEqual(format_sql_rows(result, columns), expected)

    def test_generate_insert_sql(self):
        result = [
            {"id": 1, "name": "Alice", "age": None},
            {"id": 2, "name": "Bob", "age": 30}
        ]
        expected = (
            "INSERT INTO table_name (id, name, age) VALUES\n"
            "  (1, 'Alice', NULL),\n"
            "  (2, 'Bob', 30);"
        )
        self.assertEqual(generate_export_sql(result), expected)

    def test_generate_insert_sql_empty(self):
        result = []
        self.assertIsNone(generate_export_sql(result))

    def test_clean_sql_input_normalizes(self):
        raw = "  SELECT\r\n\u00A0*  FROM\u00A0real_acct  "
        cleaned = clean_sql_input(raw)
        # no leading/trailing whitespace and windows newlines normalized
        self.assertFalse(cleaned.startswith(' '))
        self.assertFalse(cleaned.endswith(' '))
        self.assertIn('\n', cleaned)
        self.assertNotIn('\u00A0', cleaned)

    def test_validate_empty_sql(self):
        is_valid, error, parsed = validate_sql_with_sqlglot("")
        self.assertFalse(is_valid)
        self.assertEqual(error, "Empty SQL statement provided.")
        self.assertIsNone(parsed)

    def test_compute_query_depth_cte(self):
        sql = "WITH cte AS (SELECT * FROM real_acct) SELECT * FROM cte"
        parsed = sqlglot.parse_one(sql)
        depth = _compute_query_depth(parsed)
        self.assertEqual(depth, 1)

    def test_compute_query_depth_nested(self):
        sql = "SELECT * FROM (SELECT * FROM (SELECT 1) AS t2) AS t1"
        parsed = sqlglot.parse_one(sql)
        depth = _compute_query_depth(parsed)
        self.assertGreaterEqual(depth, 2)

    def test_clean_sql_input_various_unicode_and_crlf(self):
        raw = "\u00A0\u2000SELECT\r\n*\u202FFROM\u3000table\r"
        cleaned = clean_sql_input(raw)
        self.assertNotIn('\u00A0', cleaned)
        self.assertNotIn('\u2000', cleaned)
        self.assertNotIn('\u202F', cleaned)
        self.assertNotIn('\u3000', cleaned)
        self.assertIn('\n', cleaned)

    def test_comments_leading_and_block(self):
        sql1 = "-- comment\nSELECT 1"
        is_valid1, error1, parsed1 = validate_sql_with_sqlglot(sql1)
        self.assertTrue(is_valid1)
        self.assertIsNone(error1)
        self.assertTrue(parsed1.sql(comments=False).strip().upper().startswith('SELECT'))

        sql2 = "/* c */ SELECT 1"
        is_valid2, error2, parsed2 = validate_sql_with_sqlglot(sql2)
        self.assertTrue(is_valid2)
        self.assertIsNone(error2)
        self.assertTrue(parsed2.sql(comments=False).strip().upper().startswith('SELECT'))

    def test_inline_comments_removed(self):
        sql = "SELECT 1 -- remove me"
        is_valid, error, parsed = validate_sql_with_sqlglot(sql)
        self.assertTrue(is_valid)
        no_comments = parsed.sql(comments=False)
        self.assertNotIn('remove me', no_comments)

    def test_non_string_and_whitespace_only_inputs(self):
        for bad in (None, 123, '   \n  '):
            is_valid, error, parsed = validate_sql_with_sqlglot(bad)
            self.assertFalse(is_valid)
            self.assertEqual(error, "Empty SQL statement provided.")
            self.assertIsNone(parsed)

    def test_depth_boundary_allowed_and_rejected(self):
        # depth == MAX_QUERY_DEPTH should be allowed
        from .. import utils as u
        n = u.MAX_QUERY_DEPTH
        inner = 'SELECT 1'
        for i in range(n):
            inner = f"SELECT * FROM ({inner}) t{i}"
        is_valid, error, parsed = validate_sql_with_sqlglot(inner)
        self.assertTrue(is_valid, f"Should allow depth {n}: {error}")

        # depth == MAX_QUERY_DEPTH + 1 should be rejected
        inner = 'SELECT 1'
        for i in range(n + 1):
            inner = f"SELECT * FROM ({inner}) t{i}"
        is_valid2, error2, parsed2 = validate_sql_with_sqlglot(inner)
        self.assertFalse(is_valid2)
        self.assertEqual(error2, "Query is too complex (exceeds maximum depth).")

    def test_build_scope_failure_logs_and_allows(self):
        sql = 'SELECT 1'
        # patch build_scope to raise on the actual utils module
        from unittest.mock import patch
        from .. import utils as utils_module
        with patch.object(utils_module, 'build_scope', side_effect=Exception('boom')):
            with self.assertLogs('DjangoApp Utilities Module', level='WARNING') as log:
                is_valid, error, parsed = validate_sql_with_sqlglot(sql)
                self.assertTrue(is_valid)
                self.assertIsNone(error)
                self.assertIsInstance(parsed, Expression)
                # warning about unable to compute depth
                self.assertTrue(any('Could not compute query depth' in m for m in log.output))

    def test_disallowed_op_message_contains_type(self):
        sql = "INSERT INTO t VALUES (1)"
        is_valid, error, parsed = validate_sql_with_sqlglot(sql)
        self.assertFalse(is_valid)
        self.assertIn('Insert', error)

    def test_format_sql_rows_escaping(self):
        result = [{'id': 1, 'name': "O'Reilly"}]
        out = generate_export_sql(result, table_name='authors')
        self.assertIn("O''Reilly", out)

    def test_generate_unique_query_key_distinct(self):
        a = generate_unique_query_key()
        b = generate_unique_query_key()
        self.assertNotEqual(a, b)

    def test_compute_query_depth_multiple_ctes(self):
        sql = 'WITH a AS (SELECT 1), b AS (SELECT * FROM a), c AS (SELECT * FROM b) SELECT * FROM c'
        parsed = sqlglot.parse_one(sql)
        depth = _compute_query_depth(parsed)
        self.assertGreaterEqual(depth, 2)


class SQLValidationTests(TestCase):

    def test_valid_select_query(self):
        sql = "SELECT * FROM real_acct LIMIT 10"
        is_valid, error, parsed = validate_sql_with_sqlglot(sql)
        self.assertTrue(is_valid, "Expected query to be valid")
        self.assertIsNone(error)
        self.assertIsInstance(parsed, Expression)
        
    def test_valid_with_query(self):
        sql = "WITH cte AS (SELECT * FROM real_acct) SELECT * FROM cte"
        is_valid, error, parsed = validate_sql_with_sqlglot(sql)
        self.assertTrue(is_valid, "Expected query to be valid")
        self.assertIsNone(error)
        self.assertIsInstance(parsed, Expression)

    def test_invalid_query_syntax(self):
        sql = "SELEC * FROM real_acct"
        is_valid, error, parsed = validate_sql_with_sqlglot(sql)
        self.assertFalse(is_valid, "Expected query to be invalid")
        self.assertIsNotNone(error)
        self.assertIsNone(parsed)

    def test_non_select_query(self):
        sql = "DROP TABLE real_acct"
        is_valid, error, parsed = validate_sql_with_sqlglot(sql)
        self.assertFalse(is_valid, "Expected query to be invalid")
        expected = "Query contains one of unsafe operations: (Insert, Update, Delete, Drop, Alter, Create, TruncateTable, Merge, Grant, Revoke, Replace)"
        self.assertEqual(error, expected)
        self.assertIsInstance(parsed, Expression)

    def test_query_exceeding_depth_limit(self):
        sql = "SELECT * FROM (" + " FROM (" * 11 + "SELECT 1" + ")" * 12
        is_valid, error, parsed = validate_sql_with_sqlglot(sql)
        self.assertFalse(is_valid, "Expected query to be invalid")
        self.assertEqual(error, "Query is too complex (exceeds maximum depth).")
        self.assertIsInstance(parsed, Expression)

    def test_invalid_postgres_query(self):
        sql = "SELECT select FROM table;"
        with self.assertLogs("DjangoApp Utilities Module", level="ERROR") as log:
            is_valid, error, parsed = validate_sql_with_sqlglot(sql)
            self.assertFalse(is_valid, "Expected query to be invalid")
            self.assertIsNotNone(error)
            self.assertIsNone(parsed)
            self.assertGreaterEqual(len(log.output), 1, "No log output captured")
            self.assertIn("SQL parse failed", log.output[0])


class RedisSessionTests(TestCase):

    @patch("django.core.cache.cache.set")
    @patch("django.core.cache.cache.get")
    def test_cache_key_generation(self, mock_cache_get, mock_cache_set):
        # Generate a unique query key
        query_key = generate_unique_query_key()

        # Simulate setting the key in the cache
        cache.set(query_key, "test_value", timeout=300)
        mock_cache_set.assert_called_once_with(query_key, "test_value", timeout=300)

        # Simulate retrieving the key from the cache
        mock_cache_get.return_value = "test_value"
        value = cache.get(query_key)
        mock_cache_get.assert_called_once_with(query_key)

        # Assert the retrieved value matches the expected value
        self.assertEqual(value, "test_value")

    @patch("django.core.cache.cache.set")
    @patch("django.core.cache.cache.get")
    def test_cache_validation_logic(self, mock_cache_get, mock_cache_set):
        # Mock the behavior of cache.get to simulate a cache hit (return SQL string)
        mock_cache_get.return_value = "SELECT * FROM real_acct LIMIT 10"

        # Validate the SQL query using the mocked cache value (string input)
        is_valid, error, parsed = validate_sql_with_sqlglot(mock_cache_get.return_value)

        # Assert the validation result
        self.assertTrue(is_valid, "Expected query to be valid")
        self.assertIsNone(error)
        self.assertIsInstance(parsed, Expression)
