from fastapi import FastAPI
from pydantic import BaseModel, Field
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
    Start: int = Field(alias="Start ")
    Time: str = Field(alias="Time")
    Location: str = Field(alias="Location")
    Calling_Number: int = Field(alias="Calling Number")
    Called_Number: int = Field(alias="Called Number")
    Duration_s: int = Field(alias="Duration (s)")
    Answered: str = Field(alias="Answered")
    Direction: str = Field(alias="Direction")

    class Config:
        allow_population_by_field_name = True

class CallRecords(BaseModel):
    records: list[CallRecord]

@app.post("/call-test")
def call_test(record: CallRecord):
    output = {
        "start": record.Start,
        "calling_number": record.Calling_Number
    }
    return {"status": "ok", "data": output}