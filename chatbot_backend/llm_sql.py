import os
import re
import json
from functools import lru_cache
from datetime import datetime
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

OPENAI_MODEL = os.getenv("OPENAI_MODEL")
INSIGHT_MODEL = os.getenv("INSIGHT_MODEL")
SQL_TEMPERATURE = float(os.getenv("SQL_LLM_TEMPERATURE", "0.0"))
INSIGHT_TEMPERATURE = float(os.getenv("INSIGHT_LLM_TEMPERATURE", "0.2"))


# -----------------------------
# Logging helpers
# -----------------------------
def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log_error(context: str, err: Exception):
    print(f"{_ts()} | [LLM][ERROR] {context}: {err}")


# -----------------------------
# Column semantics
# -----------------------------
COLUMN_GUIDE = """
Table: slspurcinv.v_open_order
Customer fields: customer_no, name, city, customer_po_no, customer_type
Order fields: order_no, line_no, order_date, due_date, order_qty, balance_qty, balance_amount, pick_and_pack_qty, pegging, pick_list, ship_from, ship_via
Item fields: item_no, description, item_type, item_category, item_sub_category, unit_price
Sales fields: sales_rep_no, sales_rep_name
Other: internal_notes
Purchase order: po_no
"""

# -----------------------------
# Prompts
# -----------------------------
SQL_SYSTEM = """
You are a senior data analyst writing a SINGLE, safe PostgreSQL query.

CRITICAL RULES:
- NEVER generate SQL placeholders (e.g., :param, $1). ALWAYS inline literal values.
- ALWAYS produce fully executable SQL.
- Generate EXACTLY ONE statement starting with SELECT or WITH.
- NEVER modify data.
- Use the table slspurcinv.v_open_order.
- Use explicit column names.
- Use ISO dates 'YYYY-MM-DD'.
- Use ILIKE for text searches.
- Return ONLY the SQL statement. No markdown, no backticks.
"""

SQL_USER_PROMPT = """
Database table: {schema}
Column semantics: {column_guide}
Conversation context: {chat_history}
User question: {question}

Constraints:
- Use previous chat turns to resolve references.
- Add LIMIT {row_limit}.

Return ONLY the executable SQL statement:
"""

FEW_SHOTS = [
    ("Top 5 customers by total order amount",
     "SELECT customer_no, name, SUM(line_total_amount) AS total_amount FROM slspurcinv.v_open_order GROUP BY customer_no, name ORDER BY total_amount DESC LIMIT 5;"),
    ("Show orders pending for Bangalore",
     "SELECT order_no, name, city, due_date FROM slspurcinv.v_open_order WHERE city ILIKE '%Bangalore%' AND balance_qty > 0 ORDER BY due_date ASC LIMIT 50;")
]

INSIGHT_SYSTEM = """
You answer the user's question using ONLY the data found in the SQL result rows.
**Direct Answer Rule:** Detect if the question seeks a single winner (e.g., "Which customer...?"). In these cases, ignore the list format and provide the answer as a single, standalone sentence.

Formatting Rules:
- If the result has multiple rows (e.g., list of customers, orders, items), YOU MUST format them as a vertical list.
- INSERT A NEWLINE between every item. Do not list them on the same line.
- Start each item with a bullet point (-) or number.
- Format example:
  - Customer A: 5 orders
  - Customer B: 3 orders
- If the result is a single fact, provide one clear sentence.
- Do NOT mention "SQL", "query", or "database".
"""

INSIGHT_USER_PROMPT = "Question: {question}\nSQL: {sql}\nRows: {rows_json}\nInsight:"

REWRITE_SYSTEM = "Rewrite the user question into a clear, database-friendly question. Resolve vague references using history."


# -----------------------------
# LLM Factories
# -----------------------------
@lru_cache
def get_sql_llm() -> ChatOpenAI:
    return ChatOpenAI(model=OPENAI_MODEL, temperature=SQL_TEMPERATURE, api_key=os.getenv("OPENAI_API_KEY"))


@lru_cache
def get_insight_llm() -> ChatOpenAI:
    return ChatOpenAI(model=INSIGHT_MODEL, temperature=INSIGHT_TEMPERATURE, api_key=os.getenv("OPENAI_API_KEY"))


# -----------------------------
# Core Functions
# -----------------------------
def to_sql(question: str, schema_text: str, row_limit: int, chat_history_text: str = "",
           last_item: str | None = None) -> str:
    try:
        examples_txt = "\n\n".join([f"Q: {q}\nA: {a}" for q, a in FEW_SHOTS])

        extra_ctx = ""
        if last_item:
            extra_ctx = (f"\n\nContext: User previously referenced item '{last_item}'. "
                         f"If vague (e.g., 'this item'), filter for item_no ILIKE '%{last_item}%'.")

        msg = SQL_USER_PROMPT.format(
            schema=schema_text, column_guide=COLUMN_GUIDE, question=question.strip(),
            chat_history=chat_history_text, row_limit=row_limit
        )

        resp = get_sql_llm().invoke([
            SystemMessage(content=SQL_SYSTEM),
            HumanMessage(content=examples_txt + "\n\n" + msg + extra_ctx)
        ])

        return resp.content.strip().strip("`").replace("sql\n", "")

    except Exception as e:
        _log_error("to_sql", e)
        raise


def to_insight(question: str, sql: str, rows_json: str) -> str:
    try:
        prompt = INSIGHT_USER_PROMPT.format(question=question, sql=sql, rows_json=rows_json)
        resp = get_insight_llm().invoke([SystemMessage(content=INSIGHT_SYSTEM), HumanMessage(content=prompt)])
        return resp.content.strip()
    except Exception as e:
        _log_error("to_insight", e)
        return "No insights available."


def rewrite_question(question: str, chat_history_text: str, last_item: str | None = None) -> str:
    try:
        extra = f"\nKnown entity: last_item = '{last_item}'" if last_item else ""
        prompt = f"History:\n{chat_history_text}\n\nQuestion:\n{question}\n{extra}\n\nRewrite:"
        resp = get_insight_llm().invoke([SystemMessage(content=REWRITE_SYSTEM), HumanMessage(content=prompt)])
        return resp.content.strip()
    except Exception:
        return question


# -----------------------------
# Entity Extraction (Pure Functions)
# -----------------------------
def extract_item_no_from_text(text: str) -> str | None:
    if not text: return None
    up = text.upper()
    pattern = r"\b([A-Z0-9]{2,12}(?:[-_][A-Z0-9]{1,12})+)\b"
    tokens = re.findall(pattern, up)
    BLACKLIST = {"THE", "AND", "COMPANY", "LIMITED", "LTD", "INC", "LLC"}

    for token in tokens:
        if len(token) < 4 or token in BLACKLIST: continue
        if any(c.isdigit() for c in token) or "-" in token:
            return token
    return None


def extract_entity_from_sql(sql: str) -> str | None:
    """Extracts item_no literal from generated SQL."""
    if not sql: return None
    # Look for item_no = 'VALUE' or ILIKE 'VALUE'
    m = re.search(r"item_no\s*(?:=|ILIKE)\s*'([^']+)'", sql, flags=re.IGNORECASE)
    if m:
        # If ILIKE used %wrappers%, strip them
        return m.group(1).replace("%", "").upper().strip()
    return None


# -----------------------------
# Chat Chain Factory
# -----------------------------
CHAT_SYSTEM = "You are a helpful assistant. Use chat history. Avoid SQL unless asked."
chat_prompt = ChatPromptTemplate.from_messages([("system", CHAT_SYSTEM), ("human", "{input}")])
chat_chain = chat_prompt | ChatOpenAI(model=OPENAI_MODEL, temperature=0.4, api_key=os.getenv("OPENAI_API_KEY"))


def get_base_chat_chain():
    # Returns the chain without memory binding (memory handled in main.py)
    return chat_chain