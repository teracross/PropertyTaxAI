import unittest
from ..query_depth import QueryDepthAnalyzer
import sqlglot


class QueryDepthAnalyzerTests(unittest.TestCase):

    def test_cte_chain_depth(self):
        sql = 'WITH a AS (SELECT 1), b AS (SELECT * FROM a), c AS (SELECT * FROM b) SELECT * FROM c'
        analyzer = QueryDepthAnalyzer()
        parsed = sqlglot.parse_one(sql)
        depth = analyzer.compute_from_parsed(parsed)
        # Expect CTE chain to be detected as depth >= 2
        self.assertGreaterEqual(depth, 2)

    def test_nested_subquery_depth(self):
        sql = 'SELECT * FROM (SELECT * FROM (SELECT 1) t2) t1'
        analyzer = QueryDepthAnalyzer()
        parsed = sqlglot.parse_one(sql)
        depth = analyzer.compute_from_parsed(parsed)
        self.assertGreaterEqual(depth, 2)
