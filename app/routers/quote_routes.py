# app/routers/quote_routes.py

import json
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database.conection import SessionLocal
from app.utils.auth import get_current_user_id
from app.models.cases import Case
from app.models.user import User

# Importar loader desde el asistente
from app.routers.chat_routes import load_requirements_from_storage

router = APIRouter(tags=["quote"])
templates = Jinja2Templates(directory="app/templates")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/quote/{case_id}", response_class=HTMLResponse)
async def get_quote_view(
    request: Request,
    case_id: int,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    # ------------------------------------
    # 1. Validar caso
    # ------------------------------------
    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        raise HTTPException(404, "Caso no encontrado")

    current_user = db.query(User).get(current_user_id)

    # ------------------------------------
    # 2. Cargar bombas desde BD (futuro)
    # ------------------------------------
    detected_from_db = []  # por ahora vacío, en futuro: query

    # ------------------------------------
    # 3. Cargar bombas detectadas del asistente
    # ------------------------------------
    detected_from_json = load_requirements_from_storage(case_id)

    # ------------------------------------
    # 4. Construir JSON maestro
    # ------------------------------------
    pumps_payload = {
        "detected": detected_from_json or [],
        "db": [pump.to_dict() for pump in detected_from_db] if detected_from_db else []
    }

    # JSON serializado para JS (modal)
    pumps_payload_json = json.dumps(pumps_payload, ensure_ascii=False)

    # ------------------------------------
    # 5. Contexto para render Jinja
    # ------------------------------------
    context = {
        "request": request,
        "case": case,

        # Compatibilidad con HTML existente
        "requirements": detected_from_json,
        "requirements_json": detected_from_json,

        # JSON unificado para los modales (clave)
        "pumps_payload_json": pumps_payload_json,

        # Otros datos del quote
        "quote_items": [],
        "selected_client": None,
        "selected_terms": None,
        "quote_notes": "",
        "user_name": current_user.nombre,
        "user_rol": current_user.rol,
    }

    return templates.TemplateResponse(
        "assistant/_quote_final.html",
        context
    )
