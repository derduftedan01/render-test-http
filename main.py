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