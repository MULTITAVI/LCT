# Original single-file prototype moved here for reference.
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Optional
from uuid import uuid4
import uvicorn
import datetime

app = FastAPI(title="LCT Optimization Service - Prototype (legacy)")

class DDLStatement(BaseModel):
    statement: str


class QueryStat(BaseModel):
    queryid: str
    query: str
    runquantity: int


class NewTaskRequest(BaseModel):
    url: str
    ddl: List[DDLStatement]
    queries: List[QueryStat]


class TaskIdResponse(BaseModel):
    taskid: str


class StatusResponse(BaseModel):
    status: str


class ResultQuery(BaseModel):
    queryid: str
    query: str


class ResultResponse(BaseModel):
    ddl: List[DDLStatement]
    migrations: List[DDLStatement]
    queries: List[ResultQuery]


TASKS = {}


def log(msg: str):
    ts = datetime.datetime.utcnow().isoformat()
    print(f"[{ts}] {msg}")


@app.post("/new", response_model=TaskIdResponse)
def create_task(payload: NewTaskRequest):
    taskid = str(uuid4())
    TASKS[taskid] = {
        "status": "RUNNING",
        "payload": payload.dict(),
    }
    log(f"New task created: taskid={taskid}")
    TASKS[taskid]["status"] = "DONE"
    return TaskIdResponse(taskid=taskid)


@app.get("/status", response_model=StatusResponse)
def get_status(task_id: str = Query(..., alias="task_id")):
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="task_id not found")
    status = TASKS[task_id]["status"]
    log(f"Status request for taskid={task_id} -> {status}")
    return StatusResponse(status=status)


@app.get("/getresult", response_model=ResultResponse)
def get_result(task_id: str = Query(..., alias="task_id")):
    if task_id not in TASKS:
        raise HTTPException(status_code=404, detail="task_id not found")
    status = TASKS[task_id]["status"]
    if status != "DONE":
        raise HTTPException(status_code=409, detail="task not ready")
    payload = TASKS[task_id]["payload"]
    result_ddl = [{"statement": "CREATE SCHEMA catalog.newschema"}]
    migrations = [{"statement": "INSERT INTO catalog.newschema.new_table SELECT * FROM catalog.public.old_table"}]
    result_queries = []
    for q in payload.get("queries", [])[:10]:
        result_queries.append({"queryid": q.get("queryid"), "query": "SELECT * FROM catalog.newschema.new_table"})
    return ResultResponse(
        ddl=[DDLStatement(**d) for d in result_ddl],
        migrations=[DDLStatement(**m) for m in migrations],
        queries=[ResultQuery(**r) for r in result_queries],
    )


if __name__ == "__main__":
    uvicorn.run("lct.legacy_api:app", host="127.0.0.1", port=8080, log_level="info")
