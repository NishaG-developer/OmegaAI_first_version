import asyncio
import json
import os
import uuid
import time
import re
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Dict, Any
from threading import Lock

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.concurrency import run_in_threadpool  # KEY FIX for blocking I/O
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

# LangChain memory imports
from langchain_core.chat_history import InMemoryChatMessageHistory
from langchain_core.runnables import RunnableWithMessageHistory

from db_connect import get_db, get_schema_snapshot, Base, engine
from models import ChatHistory
import llm_sql
import utils

# ------------------------------
# Configuration
# ------------------------------
CLEANUP_INTERVAL_SECONDS = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "60"))
SESSION_TIMEOUT_MINUTES = int(os.getenv("SESSION_TIMEOUT_MINUTES", "30"))
ROW_LIMIT = int(os.getenv("ROW_LIMIT", "100"))


# ------------------------------
# Robust Session Manager (Scalability Prep)
# ------------------------------
class SessionManager:
    """
    Thread-safe session manager.
    To scale to multiple workers/pods, replace self._store with Redis.
    """

    def __init__(self):
        self._store: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    def get_session(self, session_id: str) -> Dict[str, Any]:
        with self._lock:
            if session_id not in self._store:
                self._store[session_id] = {
                    "history": InMemoryChatMessageHistory(),
                    "last_item": None,
                    "last_activity": datetime.now()
                }

            # Update activity
            self._store[session_id]["last_activity"] = datetime.now()
            return self._store[session_id]

    def get_history(self, session_id: str) -> InMemoryChatMessageHistory:
        return self.get_session(session_id)["history"]

    def get_last_item(self, session_id: str) -> str | None:
        return self.get_session(session_id).get("last_item")

    def set_last_item(self, session_id: str, item: str):
        with self._lock:
            if session_id in self._store:
                self._store[session_id]["last_item"] = item

    def cleanup(self, timeout_minutes: int):
        with self._lock:
            now = datetime.now()
            expired = [
                sid for sid, data in self._store.items()
                if (now - data["last_activity"]) > timedelta(minutes=timeout_minutes)
            ]
            for sid in expired:
                del self._store[sid]
            if expired:
                print(f"[CLEANUP] Removed {len(expired)} expired sessions.")


session_manager = SessionManager()


# ------------------------------
# Lifecycle & App
# ------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Initializing DB tables...")
    Base.metadata.create_all(bind=engine)

    async def background_cleanup():
        while True:
            try:
                session_manager.cleanup(SESSION_TIMEOUT_MINUTES)
            except Exception as e:
                print(f"Cleanup error: {e}")
            await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)

    task = asyncio.create_task(background_cleanup())
    yield
    task.cancel()


app = FastAPI(title="ERP Chatbot", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------
# Pydantic Models
# ------------------------------
class StartSessionResponse(BaseModel):
    session_id: str


class AskRequest(BaseModel):
    question: str
    session_id: str


class AskResponse(BaseModel):
    summary: str
    insights: str | None = None


class ChatRequest(BaseModel):
    message: str
    session_id: str | None = None


class ChatResponse(BaseModel):
    reply: str


class SmartRequest(BaseModel):
    message: str
    session_id: str | None = None


class SmartResponse(BaseModel):
    reply: str
    mode: str


def get_static_response(text: str) -> str | None:
    """
    Returns a static string if the input matches specific greetings or gratitude keywords.
    Returns None if no match is found.
    """
    t = text.lower().strip()

    # --- 1. Greetings ---
    # Matches: "hi", "hello", "hi there", "good morning", etc.
    GREETINGS = ["hi", "hello", "hey", "greetings", "good morning", "good afternoon", "good evening"]

    # Check for exact match OR if the sentence starts with a greeting followed by a space
    if t in GREETINGS or any(t.startswith(g + " ") for g in GREETINGS):
        return "Hello! I am your ERP assistant. You can ask me about open orders, items, customers, or pending shipments."

    # --- 2. Gratitude & Closing ---
    # Matches: "thank you", "thanks", "thanks a lot", "bye", "ok bye"
    GRATITUDE = ["thank", "thanks", "thx", "appreciated"]
    CLOSING = ["bye", "goodbye", "see you", "cya"]

    # 'ok' is tricky because it might be "ok show me orders".
    # We only catch 'ok' if it is the ONLY word or strictly "ok thanks".
    if t in ["ok", "okay", "cool", "great"]:
        return "You're welcome! Let me know if you need anything else."

    if any(x in t for x in GRATITUDE):
        return "You're very welcome! Happy to help."

    if any(t.startswith(x) for x in CLOSING):
        return "Goodbye! Have a great day."

    return None


def requires_context_resolution(text: str) -> bool:
    """
    Returns True if the user input implies a reference to previous chat context
    """
    t = text.lower().strip()

    pattern = r"\b(this|that|these|those|it|its|they|them|same|previous|above|last|earlier)\b"
    if re.search(pattern, t):
        return True

    # Check for extremely short follow-up queries
    words = t.split()
    if len(words) <= 3:
        return True

    return False

# ------------------------------
# Core Logic (Synchronous)
# ------------------------------
def handle_sql_query_sync(question: str, session_id: str, db: Session) -> AskResponse:
    """Synchronous core logic to be run in threadpool."""

    # 1. Retrieve Context
    history_obj = session_manager.get_history(session_id)
    # We still save the message to the DB/History object for record-keeping
    history_obj.add_user_message(question)

    # Load full history text, but we might NOT use it for generation
    chat_history_text = "\n".join([m.content for m in history_obj.messages])

    # Retrieve last entity, but we might NOT use it
    last_item = session_manager.get_last_item(session_id)

    # 2. Extract Entity from User Question (if any)
    # If the user names a NEW item explicitly, we always capture it.
    user_item = llm_sql.extract_item_no_from_text(question)
    if user_item:
        session_manager.set_last_item(session_id, user_item)
        last_item = user_item  # Update local var for immediate use

    # 3. Quick Polite Check
    static_reply = get_static_response(question)
    if static_reply:
        history_obj.add_ai_message(static_reply)
        return AskResponse(summary=static_reply, insights=None)

    # 4. CONDITIONAL CONTEXT LOGIC (The Fix)
    # ---------------------------------------------------------
    final_question = question
    history_to_use = ""  # Default to empty (fresh start)
    item_to_use = None  # Default to None (fresh start)

    # Check if the user is referring to the past ("this", "that", "it", etc.)
    if requires_context_resolution(question):
        print(f"Context trigger detected in: '{question}'. Keeping history.")
        history_to_use = chat_history_text
        item_to_use = last_item
        # Optional: Rewrite only if context is needed
        final_question = llm_sql.rewrite_question(question, chat_history_text, last_item)
    else:
        print(f"No context trigger. Fresh start.")
        # We purposely send EMPTY history and NO last_item to the SQL LLM.
        # This prevents it from seeing filters from previous turns.
        history_to_use = ""
        item_to_use = None
        final_question = question
    # ---------------------------------------------------------

    schema_text = get_schema_snapshot()

    try:
        # Pass the CONDITIONAL history and item, not the full session ones
        sql = llm_sql.to_sql(final_question, schema_text, ROW_LIMIT, history_to_use, item_to_use)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # 5. Post-Processing SQL
    # If the generated SQL contains a NEW item, update the session
    sql_item = llm_sql.extract_entity_from_sql(sql)
    if sql_item:
        session_manager.set_last_item(session_id, sql_item)
        item_to_use = sql_item  # Update for the safety filters below

    # 6. Safety & Cleanup
    # Filter cleanup should rely on the item actually used in the query generation
    allowed_items = {item_to_use} if item_to_use else None
    sql = utils.remove_hallucinated_item_filters(sql, allowed_items)

    if utils.implies_pending(question) and not utils.has_balance_filter(sql):
        sql = utils.add_pending_filter(sql)

    sql = utils.remove_bind_params(sql)
    allowed, cleaned, reason = utils.sanitize_sql(sql)

    if not allowed or not cleaned:
        fail_msg = f"Rejected SQL: {reason}"
        history_obj.add_ai_message(fail_msg)
        return AskResponse(summary=fail_msg)

    # Ensure LIMIT
    if "limit" not in cleaned.lower():
        cleaned = cleaned.rstrip(";") + f" LIMIT {ROW_LIMIT};"

    # 7. Execution
    try:
        result = db.execute(text(cleaned))
        rows = [dict(zip(result.keys(), r)) for r in result.fetchall()]
    except Exception as e:
        db.rollback()
        err_msg = f"Query failed: {str(e)}"
        history_obj.add_ai_message(err_msg)
        raise HTTPException(status_code=500, detail=err_msg)

    # 8. Insights
    if rows:
        rows_json = json.dumps(rows, default=str)
        insights = llm_sql.to_insight(question, cleaned, rows_json)
    else:
        insights = "I couldn't find any records matching your request."

    # Save to DB Log and Memory
    utils.save_chat_record(db, session_id, question, cleaned, insights)
    history_obj.add_ai_message(insights)

    summary = "Here are the results." if rows else "No matching records found."
    return AskResponse(summary=summary, insights=insights)


# ------------------------------
# Routes (Async wrappers)
# ------------------------------
@app.post("/session/start", response_model=StartSessionResponse)
async def start_session():
    sid = uuid.uuid4().hex
    session_manager.get_session(sid)  # Init
    return StartSessionResponse(session_id=sid)


@app.post("/ask", response_model=AskResponse)
async def ask(payload: AskRequest, db: Session = Depends(get_db)):
    # Run the synchronous logic in a thread to avoid blocking the event loop
    return await run_in_threadpool(handle_sql_query_sync, payload.question, payload.session_id, db)


@app.post("/chat", response_model=ChatResponse)
async def chat(payload: ChatRequest):
    session_id = payload.session_id or uuid.uuid4().hex

    # Helper for chat chain with history
    def run_chat():
        base_chain = llm_sql.get_base_chat_chain()
        chain_with_history = RunnableWithMessageHistory(
            base_chain,
            session_manager.get_history,
            input_messages_key="input",
            history_messages_key="chat_history"
        )
        res = chain_with_history.invoke({"input": payload.message}, config={"session_id": session_id})
        return res.content if hasattr(res, "content") else str(res)

    reply = await run_in_threadpool(run_chat)
    return ChatResponse(reply=reply)


@app.post("/smart", response_model=SmartResponse)
async def smart_router(payload: SmartRequest, db: Session = Depends(get_db)):
    session_id = payload.session_id or uuid.uuid4().hex
    text_in = payload.message.strip()

    # --- NEW: Check Static Response First ---
    static_reply = get_static_response(text_in)
    if static_reply:
        # If it's a greeting, we can consider it "chat" mode but with instant reply
        return SmartResponse(reply=static_reply, mode="static")
    # ----------------------------------------

    # Smart Routing Heuristics
    is_sql = False
    BUSINESS_KEYWORDS = ["order", "customer", "item", "pending", "balance", "qty", "sales", "invoice"]

    if any(k in text_in.lower() for k in BUSINESS_KEYWORDS):
        is_sql = True

    # Fallback for "help" or vague questions handled by Chat LLM
    if text_in.lower() in ["help", "what can you do"]:
        is_sql = False

    if is_sql:
        resp = await run_in_threadpool(handle_sql_query_sync, text_in, session_id, db)
        return SmartResponse(reply=resp.insights or resp.summary, mode="sql")
    else:
        # Re-use chat route logic
        def run_chat():
            base_chain = llm_sql.get_base_chat_chain()
            chain_with_history = RunnableWithMessageHistory(
                base_chain,
                session_manager.get_history,
                input_messages_key="input",
                history_messages_key="chat_history"
            )
            res = chain_with_history.invoke({"input": text_in}, config={"session_id": session_id})
            return res.content

        reply = await run_in_threadpool(run_chat)
        return SmartResponse(reply=str(reply), mode="chat")