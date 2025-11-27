from typing import Tuple
import re
from models import ChatHistory

ALLOWED_PREFIXES = ("SELECT", "WITH")
FORBIDDEN = ("DROP ", "DELETE ", "INSERT ", "UPDATE ", "ALTER ", "TRUNCATE ", "EXEC ", "CREATE ", "GRANT ", "REVOKE ")


def sanitize_sql(sql: str) -> Tuple[bool, str | None, str | None]:
    """Basic guardrail: only allow read-only queries."""
    if not sql:
        return False, None, "Empty SQL"
    cleaned = sql.strip().lstrip(";")
    fw = cleaned.split()[0].upper()
    if fw not in ALLOWED_PREFIXES:
        return False, cleaned, f"First keyword must be SELECT/WITH, got '{fw}'"
    up = cleaned.upper()
    for bad in FORBIDDEN:
        if bad in up:
            return False, cleaned, f"Forbidden keyword detected: {bad.strip()}"
    return True, cleaned, None


PENDING_KEYWORDS = (
    "pending", "backlog", "back log", "due", "overdue", "past due", "outstanding", "unshipped",
    "balance", "balance qty", "balance quantity"
)


def implies_pending(question: str) -> bool:
    q = question.lower()
    return any(k in q for k in PENDING_KEYWORDS)


def has_balance_filter(sql: str) -> bool:
    return bool(re.search(r"balance_qty\s*>\s*0", sql, flags=re.I))


def add_pending_filter(sql: str) -> str:
    """Robustly adds balance_qty > 0 to the WHERE clause."""
    sql_clean = sql.rstrip(";").strip()

    # Check if WHERE clause exists (case insensitive)
    match = re.search(r"\bWHERE\b", sql_clean, flags=re.IGNORECASE)

    if match:
        # Insert AND after the WHERE keyword
        # We replace the first instance of WHERE with WHERE balance_qty > 0 AND
        # This is safer than splitting which might break on subqueries
        start, end = match.span()
        return sql_clean[:end] + " balance_qty > 0 AND " + sql_clean[end:]
    else:
        # Check for GROUP BY, ORDER BY, LIMIT to insert WHERE before them
        # Simple heuristic: If no WHERE, append it at the end (or before suffix clauses)
        # For simplicity in generated SQL, appending often works if no suffix,
        # but to be safe, we just append it.
        # A more complex parser would be needed for perfect insertion before GROUP BY,
        # but the LLM usually puts WHERE early.

        # If the LLM generates 'SELECT ... GROUP BY...', we can't just append WHERE.
        # However, correcting malformed SQL structure is hard via regex.
        # We assume standard structure.

        # Try to find GROUP BY / ORDER BY / LIMIT to insert before
        suffix_match = re.search(r"\b(GROUP BY|ORDER BY|LIMIT)\b", sql_clean, flags=re.IGNORECASE)
        if suffix_match:
            s_start, _ = suffix_match.span()
            return f"{sql_clean[:s_start]} WHERE balance_qty > 0 {sql_clean[s_start:]}"
        else:
            return f"{sql_clean} WHERE balance_qty > 0"


def save_chat_record(db_session, session_id, user_message=None, generated_sql=None, ai_message=None):
    try:
        record = ChatHistory(
            session_id=session_id,
            user_message=user_message,
            generated_sql=generated_sql,
            ai_message=ai_message
        )
        db_session.add(record)
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        print("ORM Chat History Save Error:", e)


def remove_bind_params(sql: str) -> str:
    cleaned = re.sub(r":\w+", "''", sql)  # replace :abc with ''
    cleaned = re.sub(r"\$\d+", "''", cleaned)  # replace $1 with ''
    cleaned = re.sub(r"@\w+", "''", cleaned)  # replace @p1 with ''
    return cleaned


def remove_hallucinated_item_filters(sql: str, allowed_items: set | None = None) -> str:
    if not sql: return sql
    s = sql

    # Find all ILIKE tokens for item_no/description
    # Expanded regex to capture standard SQL string literals
    ilike_patterns = re.findall(r"(item_no|description)\s+ILIKE\s+'%?([^'%]+)%?'", s, flags=re.IGNORECASE)

    if not allowed_items:
        # Remove all item/desc filters if no context exists
        s = re.sub(r"\bAND\s*\(\s*item_no\s+ILIKE\s+'[^']+'(?:\s*OR\s*description\s+ILIKE\s+'[^']+')?\s*\)", "", s,
                   flags=re.IGNORECASE)
        s = re.sub(r"\bAND\s*item_no\s+ILIKE\s+'[^']+'", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\bAND\s*description\s+ILIKE\s+'[^']+'", "", s, flags=re.IGNORECASE)
        # Also handle WHERE clauses if they are the only filter
        s = re.sub(r"\bWHERE\s*item_no\s+ILIKE\s+'[^']+'", "WHERE 1=1", s, flags=re.IGNORECASE)
        return s

    def clause_should_stay(token: str) -> bool:
        tok = token.upper()
        for ai in allowed_items:
            # Allow partial matches if they are significant
            if ai in tok or tok in ai:
                return True
        return False

    for field, token in ilike_patterns:
        if not clause_should_stay(token):
            token_esc = re.escape(token)
            # Remove complex OR clause
            s = re.sub(
                rf"\bAND\s*\(\s*item_no\s+ILIKE\s+'%?{token_esc}%?'\s*(?:\s*OR\s*description\s+ILIKE\s+'%?{token_esc}%?')?\s*\)",
                "", s, flags=re.IGNORECASE)
            # Remove single clauses
            s = re.sub(rf"\bAND\s*item_no\s+ILIKE\s+'%?{token_esc}%?'", "", s, flags=re.IGNORECASE)
            s = re.sub(rf"\bAND\s*description\s+ILIKE\s+'%?{token_esc}%?'", "", s, flags=re.IGNORECASE)

    return s