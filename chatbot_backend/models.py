from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func
from db_connect import Base   # Your existing Base from db engine

class ChatHistory(Base):
    __tablename__ = "chat_history"
    __table_args__ = {"schema": "slspurcinv"}

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String(200), nullable=False)
    user_message = Column(Text, nullable=True)
    generated_sql = Column(Text, nullable=True)
    ai_message = Column(Text, nullable=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
