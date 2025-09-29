from fastapi import FastAPI, HTTPException, Query
from uuid import uuid4
import uvicorn

from . import store
from .schemas import (
    DDLStatement,
    QueryStat,
    NewTaskRequest,
    TaskIdResponse,
    StatusResponse,
    ResultQuery,
    ResultResponse,
)


app = FastAPI(title="LCT Optimization Service - Prototype")


@app.post("/new", response_model=TaskIdResponse)
def create_task(payload: NewTaskRequest):
    taskid = str(uuid4())
    store.TASKS[taskid] = {
        "status": "RUNNING",
        "payload": payload.dict(),
    }

    store.log(f"New task created: taskid={taskid}")
    store.log(f"Request.url: {payload.url}")
    store.log(f"DDL statements count: {len(payload.ddl)}")
    for i, d in enumerate(payload.ddl, 1):
        store.log(f"DDL[{i}]: {d.statement}")
    store.log(f"Queries count: {len(payload.queries)}")
    for q in payload.queries:
        store.log(f"Query id={q.queryid} runquantity={q.runquantity} query={q.query}")

    # TODO: Здесь заглушка — в продакшне нужно вынести обработку задачи
    # в фоновые воркеры (Celery / RQ / custom worker) и очередь сообщений (Redis / RabbitMQ).
    # - Синхронный путь должен вернуть taskid сразу, а реальная аналитика должна выполняться асинхронно.
    # - Менеджер задач должен сохранять историю статусов (RUNNING, PENDING, FAILED, DONE), таймстемпы и ошибки.
    # - Результаты анализа (DDL, миграции, переписанные запросы) нужно сохранять в БД/хранилище для последующего запроса.
    # Текущая реализация помечает задачу как DONE для прототипа.
    store.TASKS[taskid]["status"] = "DONE"
    return TaskIdResponse(taskid=taskid)


@app.get("/status", response_model=StatusResponse)
def get_status(task_id: str = Query(..., alias="task_id")):
    if task_id not in store.TASKS:
        raise HTTPException(status_code=404, detail="task_id not found")
    status = store.TASKS[task_id]["status"]
    store.log(f"Status request for taskid={task_id} -> {status}")
    return StatusResponse(status=status)


@app.get("/getresult", response_model=ResultResponse)
def get_result(task_id: str = Query(..., alias="task_id")):
    if task_id not in store.TASKS:
        raise HTTPException(status_code=404, detail="task_id not found")
    status = store.TASKS[task_id]["status"]
    store.log(f"GetResult request for taskid={task_id} status={status}")

    if status != "DONE":
        raise HTTPException(status_code=409, detail="task not ready")

    payload = store.TASKS[task_id]["payload"]
    store.log(f"Original URL: {payload.get('url')}")
    store.log(f"Original DDL count: {len(payload.get('ddl', []))}")
    store.log(f"Original queries count: {len(payload.get('queries', []))}")

    # TODO (RU): Заглушка генерации результата.
    # Здесь должен быть вызов модуля аналитики/LLM, который по входным DDL и выборке запросов
    # строит предложенные DDL-изменения, безопасные планируемые миграции и переписанные запросы.
    # Пока возвращаются фиктивные примеры для прототипа.
    result_ddl = [
        {"statement": "CREATE SCHEMA catalog.newschema"},
        {"statement": "-- example new table DDL using full path\nCREATE TABLE catalog.newschema.new_table (id bigint, data varchar)"},
    ]

    migrations = [
        {"statement": "INSERT INTO catalog.newschema.new_table SELECT * FROM catalog.public.old_table"}
    ]

    result_queries = []
    for q in payload.get("queries", [])[:10]:
        result_queries.append({"queryid": q.get("queryid"), "query": "-- rewritten query using catalog.newschema\nSELECT * FROM catalog.newschema.new_table"})

    return ResultResponse(
        ddl=[DDLStatement(**d) for d in result_ddl],
        migrations=[DDLStatement(**m) for m in migrations],
        queries=[ResultQuery(**r) for r in result_queries],
    )


if __name__ == "__main__":
    uvicorn.run("lct.main:app", host="127.0.0.1", port=8080, log_level="info")
