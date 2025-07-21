from pydantic import BaseModel
from typing import List, Dict

class QuestionRequest(BaseModel):
    pergunta: str

class AnswerResponse(BaseModel):
    sql: str
    resultado: List[Dict]
