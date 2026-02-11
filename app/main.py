from fastapi import FastAPI
from producer import *
import uvicorn

app = FastAPI()

@app.post("/register")
def add_user(user: User):
    send_to_kafka(user)


# if __name__ == "__main__":
#     uvicorn.run(app="main", host="localhost", port=8000)