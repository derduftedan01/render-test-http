from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Hello from Render!"}

@app.get("/test")
def test():
    return {"status": "ok", "info": "Dieser Endpoint funktioniert lokal UND später auf Render."}