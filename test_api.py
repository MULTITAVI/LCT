import subprocess
import time
import requests
import os
import signal

UVICORN_CMD = [
    "python3",
    "-m",
    "uvicorn",
    "api.main:app",
    "--host",
    "127.0.0.1",
    "--port",
    "8080",
]


def wait_for_server(url, timeout=10.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(url, timeout=1.0)
            return True
        except Exception:
            time.sleep(0.2)
    return False


def main():
    print("Starting uvicorn server...")
    env = os.environ.copy()
    # start server
    proc = subprocess.Popen(UVICORN_CMD, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        ready = wait_for_server("http://127.0.0.1:8080/docs", timeout=15.0)
        if not ready:
            print("Server did not become ready in time")
            return

        print("Server ready — running test requests")

        # 1) POST /new
        payload = {
            "url": "jdbc://some-endpoint/database?login=xxx&password=yyyy",
            "ddl": [{"statement": "CREATE TABLE catalog.public.t1 (id bigint)"}],
            "queries": [{"queryid": "q1", "query": "SELECT * FROM t1", "runquantity": 10}],
        }
        r = requests.post("http://127.0.0.1:8080/new", json=payload)
        print("POST /new ->", r.status_code, r.text)

        taskid = None
        if r.status_code == 200:
            taskid = r.json().get("taskid")

        # 2) GET /status
        if taskid:
            r2 = requests.get(f"http://127.0.0.1:8080/status?task_id={taskid}")
            print("GET /status ->", r2.status_code, r2.text)

            # 3) GET /getresult
            r3 = requests.get(f"http://127.0.0.1:8080/getresult?task_id={taskid}")
            print("GET /getresult ->", r3.status_code, r3.text)
        else:
            print("No taskid returned from /new; skipping status/result tests")

    finally:
        print("Stopping server")
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
