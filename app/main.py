import os
from dotenv import load_dotenv
from pathlib import Path
from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from app.routes.auth import router as auth_router

env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
app = FastAPI()

app.add_middleware(SessionMiddleware, secret_key=os.getenv("SECRET_KEY"))
app.include_router(auth_router)
