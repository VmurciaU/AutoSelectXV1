# archivo: utils/auth.py

from fastapi import Request
from itsdangerous import URLSafeSerializer

# Debe coincidir con el valor usado en user_routes.py
SECRET_KEY = "autoselectx_secret_key_2024"
serializer = URLSafeSerializer(SECRET_KEY, salt="session")

def get_current_user_id(request: Request) -> int | None:
    token = request.cookies.get("session_token")
    if not token:
        return None
    try:
        user_id = serializer.loads(token)
        return int(user_id)
    except Exception:
        return None
