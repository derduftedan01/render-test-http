from fastapi import FastAPI
from pydantic import BaseModel
import gero_http

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello from Render!"}

@app.get("/test")
def test():
    return {"status": "ok", "info": "Dieser Endpoint funktioniert lokal UND später auf Render."}

class GeroInput(BaseModel):
    name: str
    value: int

@app.post("/run-gero")
def run_gero(data: GeroInput):
    result = gero_http.process_data(data.model_dump())
    return {"status": "success", "output": result}

class CallRecord(BaseModel):
    Start: int
    Time: str
    Location: str
    Calling_Number: int
    Called_Number: int
    Duration_s: int
    Answered: str
    Direction: str

class CallRecords(BaseModel):
    records: list[CallRecord]

@app.post("/call-test")
def call_test(record: CallRecord):
    output = {
        "start": record.Start,
        "calling_number": record.Calling_Number
    }
    return {"status": "ok", "data": output}