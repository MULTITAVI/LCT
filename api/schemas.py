from pydantic import BaseModel
from typing import List


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
