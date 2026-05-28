from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi import Request
from pydantic import BaseModel, Field, field_validator
from uuid import UUID, uuid4
from typing import Annotated
import datetime
from pathlib import Path
import json
import logging

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

existing_ids=set()

metrics={
    "total_requests":0,
    "accepted_events":0,
    "rejected_events":0,
    "duplicate_events":0
}

DATA_FILE = Path("data/raw/news.jsonl")
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

class NewsEvent(BaseModel):
    id: UUID 
    headline:Annotated[str,Field(...,description="headline of the data",example="This is a headline")]
    source:Annotated[str,Field(...,description="source of the data",example="This is a source")]
    timestamp:Annotated[datetime.datetime,Field(...,description="timestamp of the data",example=str(datetime.datetime.now()))]

    @field_validator('headline')
    @classmethod
    def validate_headline(cls,value:str):
        if not value.strip():
            raise ValueError("headline cannot be empty")
        return value
    
    

@app.post("/ingest")
def ingest(news:NewsEvent):
    metrics["total_requests"]+=1

    if news.id in existing_ids:
        metrics["duplicate_events"]+=1
        logger.warning(f"Duplicate event with id {news.id} rejected.")
        return {
            'status': 'rejected',
            'message': f"Duplicate event with id {news.id} rejected."
        }
    
    existing_ids.add(news.id)
    metrics["accepted_events"]+=1
    with DATA_FILE.open("a") as f:
        f.write(json.dumps(news.model_dump(mode='json')) + "\n")

    logger.info(f"Event with id {news.id} accepted and stored.")
    return {
        'status': 'accepted',
        'message': f"Event with id {news.id} accepted and stored."
    }

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    metrics["total_requests"]+=1
    metrics["rejected_events"]+=1
    logger.error(f"Validation error: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={
            'status': 'rejected',
            'message': 'Validation error',
            'details': exc.errors()
        }
    ) 

@app.get("/metrics")
def get_metrics():
    return metrics