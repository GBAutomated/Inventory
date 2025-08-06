from fastapi import APIRouter, Request
from starlette.responses import RedirectResponse
from authlib.integrations.starlette_client import OAuth
from dotenv import load_dotenv
import os, requests

# Cargar variables desde .env
load_dotenv()

router = APIRouter()

# OAuth configuration
oauth = OAuth()
oauth.register(
    name='google',
    client_id=os.getenv('GOOGLE_CLIENT_ID'),
    client_secret=os.getenv('GOOGLE_CLIENT_SECRET'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

# Variables de entorno
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8000/auth/callback")
STREAMLIT_URL = os.getenv("STREAMLIT_URL", "http://localhost:8501")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}

def user_is_active(email: str) -> bool:
    response = requests.get(
        f"{SUPABASE_URL}/rest/v1/Users?email=eq.{email}&activo=is.true",
        headers=HEADERS
    )
    return response.ok and bool(response.json())

@router.get("/login")
async def login(request: Request):
    return await oauth.google.authorize_redirect(request, REDIRECT_URI)

@router.get("/auth/callback")
async def auth_callback(request: Request):
    token = await oauth.google.authorize_access_token(request)
    user_info = await oauth.google.userinfo(token=token)
    user_email = user_info['email']

    if user_is_active(user_email):
        return RedirectResponse(url=f"{STREAMLIT_URL}/auth/callback?email={user_email}")
    else:
        return RedirectResponse(url=f"{STREAMLIT_URL}/unauthorized")
