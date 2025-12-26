from typing import Optional
import re
import logging

from sqlglot import parse_one
from sqlglot import errors as sql_errors
from sqlglot.expressions import Expression
from sqlglot.optimizer.scope import build_scope

logger = logging.getLogger("DjangoApp Utilities Module")

class QueryDepthAnalyzer:
    """Analyze SQL `Expression` depth using sqlglot's scope tree and a
    heuristic to account for CTE dependency chains.

    This consolidated implementation preserves dependency-injection for the
    scope builder (useful in tests), attempts to reuse any prebuilt
    `parsed.scope` when present, and falls back to reparsing text if
    scope construction fails. `compute_from_parsed` returns an `int` depth.
    `compute_from_sql` returns `Optional[int]` (None on parse error).
    """

    def __init__(self, scope_builder=build_scope, logger_obj: Optional[logging.Logger] = None):
        self.scope_builder = scope_builder
        self.logger = logger_obj or logger

    def compute_from_sql(self, sql: str) -> Optional[int]:
        try:
            parsed = parse_one(sql, read="postgres")
        except sql_errors.ParseError as e:
            self.logger.error("SQL parse failed in compute_from_sql: %s", e)
            return None

        return self.compute_from_parsed(parsed)

    def compute_from_parsed(self, parsed: Expression) -> int:
        # Try to reuse any scope already attached to the parsed tree.
        root_scope = None
        try:
            root_scope = getattr(parsed, 'scope', None) or getattr(parsed.find(), 'scope', None)
        except Exception:
            # ignore and fall back to building via scope_builder
            root_scope = None

        if root_scope is None:
            try:
                root_scope = self.scope_builder(parsed)
            except Exception:
                # As a last resort attempt reparsing the SQL text and rebuilding scope
                try:
                    text = parsed.sql()
                    reparsed = parse_one(text)
                    root_scope = self.scope_builder(reparsed)
                except Exception:
                    self.logger.warning("Could not compute query depth; scope construction failed.")
                    return 0

        if root_scope is None:
            return 0

        # Compute maximum nested scope depth using parent pointers for robustness
        max_depth = 0
        try:
            for scope in root_scope.traverse():
                depth = 0
                cur = scope
                while getattr(cur, 'parent', None) is not None:
                    depth += 1
                    cur = cur.parent
                max_depth = max(max_depth, depth)
        except Exception:
            # fallback to safe value
            self.logger.debug('Scope traversal failed; assuming depth 0')
            max_depth = 0

        # Heuristic CTE analysis (robust): capture dependency chains between CTEs
        try:
            sql_text = parsed.sql(comments=False)
            m = re.search(r"\bWITH\b", sql_text, flags=re.IGNORECASE)
            cte_chain_depth = 0
            if m:
                start = m.end()
                remaining = sql_text[start:]
                # Find end of WITH clause by locating where top-level CTE list ends
                paren = 0
                end_offset = None
                for idx, ch in enumerate(remaining):
                    if ch == '(':
                        paren += 1
                    elif ch == ')':
                        paren -= 1
                    if paren == 0:
                        # check if next token is a SELECT (start of main query)
                        k = idx + 1
                        while k < len(remaining) and remaining[k].isspace():
                            k += 1
                        if remaining[k:k+6].upper() == 'SELECT':
                            end_offset = start + k
                            break
                if end_offset is None:
                    with_clause = remaining
                else:
                    with_clause = sql_text[start:end_offset]

                # split top-level comma-separated CTE definitions
                ctes = []
                buf = ''
                depth_p = 0
                for ch in with_clause:
                    if ch == '(':
                        depth_p += 1
                    elif ch == ')':
                        depth_p -= 1
                    if ch == ',' and depth_p == 0:
                        if buf.strip():
                            ctes.append(buf.strip())
                        buf = ''
                    else:
                        buf += ch
                if buf.strip():
                    ctes.append(buf.strip())

                # parse CTE defs into name -> body mapping
                names = []
                bodies = {}
                for c in ctes:
                    parts = re.split(r"\bAS\b", c, flags=re.IGNORECASE)
                    if len(parts) >= 2:
                        name = parts[0].strip()
                        name = name.split()[-1].strip()
                        body = 'AS'.join(parts[1:]).strip()
                        names.append(name)
                        bodies[name] = body

                # build dependency graph by textual reference (simple heuristic)
                graph = {n: set() for n in names}
                for n, body in bodies.items():
                    for target in names:
                        if target == n:
                            continue
                        if re.search(rf"\b{re.escape(target)}\b", body):
                            graph[n].add(target)

                # compute longest path in graph
                visited = {}

                def dfs(node):
                    if node in visited:
                        return visited[node]
                    maxlen = 1
                    for nxt in graph.get(node, ()): 
                        maxlen = max(maxlen, 1 + dfs(nxt))
                    visited[node] = maxlen
                    return maxlen

                for n in names:
                    cte_chain_depth = max(cte_chain_depth, dfs(n))

            return max(max_depth, cte_chain_depth)
        except Exception:
            self.logger.warning('Could not compute query depth CTE heuristic; returning scope depth.')
            return max_depth