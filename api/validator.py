import contextlib
import threading
from typing import Optional

import trino


TRINO_JDBC_URL = "jdbc:trino://trino.czxqx2r9.data.bizmrg.com:443?user=hackuser&password=dovq(ozaq8ngt)oS"


def _parse_jdbc_url(jdbc_url: str) -> dict:
    """
    Parse a minimal subset of a JDBC-like Trino URL into parameters usable by trino.dbapi.
    Expected format: jdbc:trino://host:port?user=...&password=...
    Catalog/schema are not required for EXPLAIN TYPE VALIDATE.
    """
    assert jdbc_url.startswith("jdbc:trino://"), "Unsupported URL scheme"
    tail = jdbc_url[len("jdbc:trino://") :]
    if "?" in tail:
        host_port, query = tail.split("?", 1)
    else:
        host_port, query = tail, ""
    if ":" in host_port:
        host, port_str = host_port.split(":", 1)
        port = int(port_str)
    else:
        host, port = host_port, 443
    params = {}
    if query:
        for kv in query.split("&"):
            if not kv:
                continue
            if "=" in kv:
                k, v = kv.split("=", 1)
                params[k] = v
            else:
                params[kv] = ""
    return {
        "host": host,
        "port": port,
        "user": params.get("user"),
        "http_scheme": "https" if port == 443 else "http",
        "password": params.get("password"),
    }


def is_sql_valid_trino(sql: str, jdbc_url: Optional[str] = None, timeout_seconds: float = 5.0) -> bool:
    """
    Return True if SQL is valid for Trino by running `EXPLAIN (TYPE VALIDATE) <sql>` with a strict timeout.

    This does not execute the query; it only asks Trino to validate semantics against the catalog.
    """
    if not sql or not sql.strip():
        return False

    params = _parse_jdbc_url(jdbc_url or TRINO_JDBC_URL)

    # Use a separate thread to enforce a hard timeout on the DB call
    result = {"ok": False}

    def worker():
        try:
            auth = None
            if params.get("password"):
                auth = trino.auth.BasicAuthentication(params["user"], params["password"])  # type: ignore
            # Trim trailing semicolons; the Trino driver expects a single statement
            trimmed_sql = sql.strip().rstrip(";")

            conn = trino.dbapi.connect(
                host=params["host"],
                port=params["port"],
                user=params.get("user"),
                http_scheme=params.get("http_scheme", "https"),
                auth=auth,
                source="sql-validator",
                # Bound execution time at the engine level as well
                session_properties={
                    "query_max_execution_time": f"{int(max(1, timeout_seconds - 1))}s",
                    "query_max_run_time": f"{int(max(1, timeout_seconds - 1))}s",
                },
            )
            with contextlib.closing(conn):
                cur = conn.cursor()
                with contextlib.closing(cur):
                    # Warm-up lightweight no-op to initialize TLS/session quickly
                    try:
                        cur.execute("SELECT 1")
                        _ = cur.fetchall()
                    except Exception:
                        pass

                    # Trino supports EXPLAIN (TYPE VALIDATE) to validate without execution
                    cur.execute(f"EXPLAIN (TYPE VALIDATE) {trimmed_sql}")
                    # We expect either no error or a single row explain output
                    _ = cur.fetchall()
                    result["ok"] = True
        except Exception:
            result["ok"] = False

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout=timeout_seconds)

    if thread.is_alive():
        # Timed out; best-effort cancel by returning False
        return False

    return bool(result["ok"])

