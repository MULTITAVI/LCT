#!/usr/bin/env python3

import re, os
from typing import TypedDict, Optional, Dict
from langgraph.graph import StateGraph, END
from openai import OpenAI
from dotenv import load_dotenv


# --------- Конфигурация OpenAI API ----------
load_dotenv(".env")

API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
LLM_NAME = os.getenv("LLM_NAME")

client = OpenAI(
    base_url=API_URL,
    api_key=API_KEY,
)

# --------- Встроенный образец входа (DDL + один SQL) ----------
DDL = """
CREATE TABLE flights.public.flights (
    FlightDate date,
    Airline varchar,
    Origin varchar,
    Dest varchar,
    Cancelled boolean,
    DepDelayMinutes double,
    ArrDelayMinutes double,
    Year integer,
    Month integer
) WITH (format = 'PARQUET', format_version = 2)
"""

ORIGINAL_SQL = """
SELECT Airline, COUNT(*) AS FlightCount
FROM flights.public.flights
WHERE FlightDate >= DATE '2023-01-01'
GROUP BY Airline
ORDER BY FlightCount DESC
"""

# --------- Состояние графа ----------
class State(TypedDict):
    ddl: str
    sql_original: str
    full_table: Optional[str]
    has_year: bool
    has_month: bool
    date_filter: Optional[str]      # например "2023-01-01"
    sql_optimized: Optional[str]
    notes: str

# --------- Узлы как LLM-агенты ---------
def llm_agent(prompt: str, state: dict) -> dict:
    print(f"\n[LLM NODE] Запрос к агенту: {prompt.strip().splitlines()[0]}")
    print("[LLM NODE] Входное состояние:", state)
    messages = [
        {
            "role": "system",
            "content": [
                {"type": "text", "text": prompt}
            ]
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": str(state)}
            ]
        }
    ]
    completion = client.chat.completions.create(
        extra_body={},
        model=LLM_NAME,
        temperature=0.2,
        messages=messages
    )
    print("[LLM NODE] Ответ LLM:", completion.choices[0].message.content)
    state["llm_response"] = completion.choices[0].message.content
    return state

SCHEMA_PROMPT = """
Ты — агент для анализа схемы таблицы SQL. Получи DDL, определи полный путь к таблице, наличие Year/Month.
Верни обновлённое состояние.
"""
PATTERN_PROMPT = """
Ты — агент для анализа SQL-запроса. Найди фильтр по дате, определи значение фильтра.
Верни обновлённое состояние.
"""
OPTIMIZER_PROMPT = """
Ты — агент-оптимизатор SQL. Если есть Year/Month и фильтр по дате, перепиши WHERE для партиционного отсечения.
Верни обновлённое состояние.
"""
REPORT_PROMPT = """
Ты — агент-репортёр. Сформируй отчёт по оригинальному и оптимизированному SQL, а также применённым правилам.
Верни обновлённое состояние.
"""

def schema_analyzer(state: State) -> State:
    return llm_agent(SCHEMA_PROMPT, state)

def query_pattern(state: State) -> State:
    return llm_agent(PATTERN_PROMPT, state)

def optimizer(state: State) -> State:
    return llm_agent(OPTIMIZER_PROMPT, state)

def reporter(state: State) -> State:
    print("\n=== ИТОГОВОЕ СОСТОЯНИЕ ГРАФА ===")
    for k, v in state.items():
        print(f"{k}: {v}")
    return state

# --------- Сборка графа ----------
def build_graph():
    g = StateGraph(State)
    g.add_node("schema", schema_analyzer)
    g.add_node("pattern", query_pattern)
    g.add_node("opt", optimizer)
    g.add_node("report", reporter)
    g.set_entry_point("schema")
    g.add_edge("schema", "pattern")
    g.add_edge("pattern", "opt")
    g.add_edge("opt", "report")
    g.add_edge("report", END)
    return g.compile()

# --------- Пример запуска ----------
if __name__ == "__main__":
    initial: State = {
        "ddl": DDL,
        "sql_original": ORIGINAL_SQL,
        "full_table": None,
        "has_year": False,
        "has_month": False,
        "date_filter": None,
        "sql_optimized": None,
        "notes": "",
    }
    app = build_graph()
    app.invoke(initial, config={"recursion_limit": 10})
