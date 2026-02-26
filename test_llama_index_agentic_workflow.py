import os
import logging
from pathlib import Path
import json
from typing import Literal, Optional

from llama_index.core.workflow import Workflow, Context, StartEvent, StopEvent, step, Event
from llama_index.core.agent.workflow import ReActAgent, AgentWorkflow
from llama_index.llms.openai_like import OpenAILike

# Импортируем валидатор
from api.validator import is_sql_valid_trino

# --- Логирование ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SQLAgentWorkflow")

# --- Загрузка данных ---
API_URL = os.getenv("API_URL", "http://192.168.10.22:8110/v1")
API_KEY = os.getenv("API_KEY", "")
LLM_NAME = os.getenv("LLM_NAME", "Qwen3-Coder-30B-A3B-Instruct")

print("API_URL:", API_URL)
print("LLM_NAME:", LLM_NAME)

llm = OpenAILike(
    model=LLM_NAME,
    api_base=API_URL,
    api_key=API_KEY,
    is_function_calling_model=True,
)

ddl_best_practices = Path("api/ddl_best_practices.md").read_text()
sql_best_practices = Path("api/sql_best_practices.md").read_text()


# --- События ---
class RequestTypeEvent(Event):
    request_type: Literal["sql", "ddl"]
    query: str


class DDLReadyEvent(Event):
    ddl: str


class SQLOptimizedEvent(Event):
    sql: str


class ValidationEvent(Event):
    query_type: Literal["sql", "ddl"]
    query: str
    is_valid: bool
    attempt: int


# --- Инструменты (Tools) ---

def validate_query(query_type: Literal["sql", "ddl"], query: str) -> bool:
    """
    Валидация SQL или DDL через Trino.
    """
    logger.info(f"🔍 Валидация {query_type.upper()}: {query[:100]}...")
    result = is_sql_valid_trino(query_type, query)
    logger.info(f"✅ Валидация {'успешна' if result else 'провалена'} для {query_type.upper()}")
    return result


# --- Агенты ---

# 1. DDL-эксперт
ddl_agent = ReActAgent(
    name="ddl_optimizer",
    description="Оптимизирует DDL-запросы для Trino с учётом best practices",
    system_prompt=f"""
Вы — эксперт по DDL в Trino/Presto. Оптимизируйте CREATE TABLE запрос.

Правила:
- Используйте Parquet.
- Учитывайте best practices:
{ddl_best_practices}

Выводите ТОЛЬКО валидный DDL-запрос, без пояснений.
""",
    tools=[],  # без инструментов — только генерация
    llm=llm,
)

# 2. SQL-эксперт
sql_agent = ReActAgent(
    name="sql_optimizer",
    description="Оптимизирует SQL-запросы для Trino с учётом DDL и best practices",
    system_prompt=f"""
Вы — эксперт по аналитическим SQL-запросам в Trino.

Учитывайте:
- Структуру таблиц из DDL (если предоставлена).
- Best practices:
{sql_best_practices}

Выводите ТОЛЬКО валидный SQL-запрос, без пояснений.
""",
    tools=[],
    llm=llm,
)

# 3. Валидатор (использует функцию как инструмент)
validator_agent = ReActAgent(
    name="validator",
    description="Проверяет корректность SQL или DDL через Trino",
    system_prompt="Вы — строгий валидатор. Используйте инструмент validate_query для проверки.",
    tools=[validate_query],
    llm=llm,
)


# --- Workflow ---
class SQLAgentWorkflow(Workflow):
    max_attempts: int = 3

    @step
    async def route_request(self, ctx: Context, ev: StartEvent) -> RequestTypeEvent:
        request_type = ev.get("request_type")
        query = ev.get("query")
        if request_type not in ("sql", "ddl") or not query:
            raise ValueError("Нужны 'request_type' (sql/ddl) и 'query'")
        
        ctx.store.set("original_query", query)
        ctx.store.set("request_type", request_type)
        ctx.store.set("attempt", 0)

        logger.info(f"📥 Получен запрос типа '{request_type}'")
        logger.info(f"Запрос:\n{query}")

        return RequestTypeEvent(request_type=request_type, query=query)

    @step
    async def process_ddl(self, ctx: Context, ev: RequestTypeEvent) -> ValidationEvent:
        if ev.request_type != "ddl":
            # Передаём SQL-агенту
            return await self.process_sql(ctx, ev)

        attempt = await ctx.store.get("attempt", 0)  # 0 — значение по умолчанию
        attempt += 1
        await ctx.store.set("attempt", attempt)

        logger.info(f"🛠️ [DDL] Попытка {attempt}/{self.max_attempts}")
        response = await ddl_agent.achat(f"Оптимизируй DDL:\n{ev.query}")
        optimized_ddl = str(response.response).strip()

        logger.info(f"📝 [DDL] Оптимизированный DDL:\n{optimized_ddl}")

        # Сохраняем для SQL-агента (если понадобится)
        ctx.store.set("last_ddl", optimized_ddl)

        is_valid = validate_query("ddl", optimized_ddl)
        return ValidationEvent(
            query_type="ddl",
            query=optimized_ddl,
            is_valid=is_valid,
            attempt=attempt
        )

    @step
    async def process_sql(self, ctx: Context, ev: RequestTypeEvent) -> ValidationEvent:
        attempt = await ctx.store.get("attempt", 0)  # 0 — значение по умолчанию
        attempt += 1
        await ctx.store.set("attempt", attempt)

        # Добавляем DDL в контекст для SQL-агента, если есть
        ddl_context = ctx.store.get("last_ddl") or ctx.store.get("original_query") if ctx.store.get("request_type") == "ddl" else ""
        prompt = f"Оптимизируй SQL:\n{ev.query}"
        if ddl_context:
            prompt = f"DDL таблиц:\n{ddl_context}\n\n{prompt}"

        logger.info(f"🛠️ [SQL] Попытка {attempt}/{self.max_attempts}")
        if ddl_context:
            logger.info(f"📎 DDL-контекст доступен для SQL-агента")

        response = await sql_agent.achat(prompt)
        optimized_sql = str(response.response).strip()

        logger.info(f"📝 [SQL] Оптимизированный SQL:\n{optimized_sql}")

        is_valid = validate_query("sql", optimized_sql)
        return ValidationEvent(
            query_type="sql",
            query=optimized_sql,
            is_valid=is_valid,
            attempt=attempt
        )

    @step
    async def validate_and_loop(self, ctx: Context, ev: ValidationEvent) -> StopEvent | RequestTypeEvent:
        if ev.is_valid:
            logger.info(f"✅ Запрос прошёл валидацию с {ev.attempt} попытки")
            return StopEvent(result={"query_type": ev.query_type, "query": ev.query})

        if ev.attempt >= self.max_attempts:
            logger.error(f"❌ Превышено макс. число попыток ({self.max_attempts})")
            return StopEvent(result={
                "query_type": ev.query_type,
                "query": ev.query,
                "error": "validation_failed_after_max_attempts"
            })

        logger.warning(f"🔁 Валидация провалена. Перезапуск оптимизации (попытка {ev.attempt + 1})")
        # Возвращаемся к оптимизации
        return RequestTypeEvent(request_type=ev.query_type, query=ev.query)


# --- Запуск ---
async def main():
    # Пример: обработка SQL
    TEST_REQUEST_TYPE = "sql"
    TEST_QUERY = """
    SELECT sci.registration_source, 
           COUNT(*) AS registered_users, 
           COUNT(sci.first_purchase_date) AS buyers, 
           ROUND(COUNT(sci.first_purchase_date) * 100.0 / COUNT(*), 2) AS conversion_rate 
    FROM quests.public.s_client_personal_info sci 
    GROUP BY sci.registration_source 
    ORDER BY conversion_rate DESC;
    """.strip()

    workflow = SQLAgentWorkflow(timeout=180, verbose=True)
    result = await workflow.run(request_type=TEST_REQUEST_TYPE, query=TEST_QUERY)

    print("\n" + "="*70)
    print("🏁 ФИНАЛЬНЫЙ РЕЗУЛЬТАТ:")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())