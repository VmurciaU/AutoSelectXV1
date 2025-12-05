# app/routers/quote_routes.py

import json
import decimal
from datetime import datetime, date

from fastapi import (
    APIRouter, Request, Depends, HTTPException, Form
)
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.database.conection import SessionLocal
from app.utils.auth import get_current_user_id
from app.models.cases import Case
from app.models.user import User
from app.models.pumps_detected import PumpsDetected

# Loader del asistente
from app.routers.chat_routes import load_requirements_from_storage

router = APIRouter(tags=["quote"])
templates = Jinja2Templates(directory="app/templates")


# ================================================
# CONEXIÓN A BD
# ================================================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# -----------------------------------------------
# Helper: serializador seguro para json.dumps
# -----------------------------------------------
def _json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # Cualquier otro tipo raro:
    return str(obj)


# -----------------------------------------------
# Helper: normalizar lista de bombas detectadas
# -----------------------------------------------
def _normalize_detected_list(raw):
    """
    Asegura que lo que se pasa al template sea SIEMPRE una lista de items.
    Si viene None, string JSON o dict, se adapta.
    """
    if raw is None:
        return []

    # Si ya es lista, la dejamos
    if isinstance(raw, list):
        return raw

    # Si viene como string JSON
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return []

        # Si el JSON es lista
        if isinstance(parsed, list):
            return parsed
        # Si es dict con "items" o similar, intenta sacar lista
        if isinstance(parsed, dict):
            # Ajusta aquí si sabes la clave real
            if "items" in parsed and isinstance(parsed["items"], list):
                return parsed["items"]
            return []

    # Si es dict simple o cualquier otra cosa, no nos arriesgamos
    if isinstance(raw, dict):
        # Caso extremo: si tiene una clave "detected" que es lista
        if "detected" in raw and isinstance(raw["detected"], list):
            return raw["detected"]
        return []

    # Fallback
    return []


# ======================================================
# GET PRINCIPAL – Vista de cotización
# ======================================================
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

    # ------------------------------------
    # 2. Validar usuario actual
    # ------------------------------------
    current_user = db.get(User, current_user_id)
    if not current_user:
        # Mejor 401 que un AttributeError silencioso
        raise HTTPException(401, "Usuario no encontrado o sesión inválida")

    # ------------------------------------
    # 3. Cargar bombas detectadas desde BD
    # ------------------------------------
    detected_from_db = (
        db.query(PumpsDetected)
        .filter(
            PumpsDetected.case_id == case_id,
            PumpsDetected.is_active == True
        )
        .all()
    )
    has_pumps_in_db = len(detected_from_db) > 0

    # ------------------------------------
    # 4. Cargar bombas detectadas del asistente (JSON / storage)
    # ------------------------------------
    raw_detected = load_requirements_from_storage(case_id)

    # Normalizamos para que SIEMPRE sea lista
    detected_from_json = _normalize_detected_list(raw_detected)

    # ------------------------------------
    # 5. Construir JSON maestro para JS (modales)
    # ------------------------------------
    # OJO: aquí asumimos que PumpsDetected.to_dict() existe
    #      y devuelve tipos básicos (int, float, str, etc.).
    #      Aun así, por si hay Decimal/fechas, usamos _json_default.
    try:
        pumps_payload = {
            "detected": detected_from_json or [],
            "db": [pump.to_dict() for pump in detected_from_db],
        }
        pumps_payload_json = json.dumps(
            pumps_payload,
            ensure_ascii=False,
            default=_json_default,
        )
    except Exception as e:
        # Si algo raro pasa, no reventamos la vista:
        # dejamos payload vacío para que al menos el HTML cargue.
        print(f"[quote_routes] ERROR serializando pumps_payload: {e}")
        pumps_payload_json = json.dumps(
            {"detected": [], "db": []},
            ensure_ascii=False
        )

    # ------------------------------------
    # 6. Contexto render
    # ------------------------------------
    context = {
        "request": request,
        "case": case,

        # Para panel JSON
        "requirements": detected_from_json,
        "requirements_json": detected_from_json,

        # Para panel DB
        "pumps_from_db": detected_from_db,
        "has_pumps_in_db": has_pumps_in_db,

        # JSON maestro para JS
        "pumps_payload_json": pumps_payload_json,

        # Otros elementos del banner de cotización
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


# ======================================================
# POST – Guardar Bombas Detectadas (única vez)
# ======================================================
@router.post("/quote/{case_id}/save-detected-pumps")
async def save_detected_pumps(
    case_id: int,
    pumps_payload_json: str = Form(...),
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):

    # 1. Validar caso
    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        raise HTTPException(404, "Caso no encontrado")

    # 2. Verificar si ya existen bombas activas
    existing = db.query(PumpsDetected).filter(
        PumpsDetected.case_id == case_id,
        PumpsDetected.is_active == True
    ).count()

    # Regla: si hay bombas activas → NO dejar guardar
    if existing > 0:
        raise HTTPException(
            400,
            "Ya existen bombas detectadas guardadas para este caso."
        )

    # 3. Parsear JSON recibido
    try:
        payload = json.loads(pumps_payload_json)
        detected_list = payload.get("detected", [])
        if not isinstance(detected_list, list):
            detected_list = []
    except Exception as e:
        raise HTTPException(400, f"JSON inválido: {e}")

    # 4. Guardar cada bomba en BD
    for item in detected_list:
        # item es dict; usamos .get en cada clave
        pump = PumpsDetected(
            case_id=case_id,
            created_by=current_user_id,
            updated_by=current_user_id,

            fluid=item.get("fluid"),
            viscosity=item.get("viscosity"),

            discharge_pressure=item.get("discharge_pressure"),
            discharge_pressure_std=item.get("discharge_pressure_std"),

            flow_min=(item.get("flow") or {}).get("min"),
            flow_nominal=(item.get("flow") or {}).get("nominal"),
            flow_max=(item.get("flow") or {}).get("max"),

            flow_max_std=item.get("flow_max_std"),
            flow_unit_std=item.get("flow_unit_std"),

            cantidad_bombas=item.get("cantidad_bombas") or 1,
            estado=item.get("estado") or "borrador",

            description=(item.get("optional") or {}).get("description"),
            tag=(item.get("optional") or {}).get("tag"),
            service=(item.get("optional") or {}).get("service"),
            temperature=(item.get("optional") or {}).get("temperature"),
            materials=(item.get("optional") or {}).get("materials"),
            area=(item.get("optional") or {}).get("area"),
            location=(item.get("optional") or {}).get("location"),
        )
        db.add(pump)

    db.commit()

    # 5. Redirigir a GET
    return RedirectResponse(
        url=f"/quote/{case_id}",
        status_code=303
    )


# ======================================================
# POST – Actualizar bomba detectada desde BD
# ======================================================
@router.post("/quote/{case_id}/detected-pump/{pump_id}/update")
async def update_detected_pump(
    case_id: int,
    pump_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):

    pump = db.query(PumpsDetected).filter(
        PumpsDetected.id == pump_id,
        PumpsDetected.case_id == case_id,
        PumpsDetected.is_active == True
    ).first()

    if not pump:
        raise HTTPException(404, "Bomba no encontrada o inactiva.")

    form = await request.form()

    # Actualizar campos
    pump.updated_by = current_user_id
    pump.touch()  # asumimos que existe este método en el modelo

    pump.fluid = form.get("fluid")
    pump.viscosity = form.get("viscosity")

    pump.discharge_pressure = form.get("discharge_pressure")
    pump.discharge_pressure_std = form.get("discharge_pressure_std")

    pump.flow_min = form.get("flow_min")
    pump.flow_nominal = form.get("flow_nominal")
    pump.flow_max = form.get("flow_max")

    pump.flow_max_std = form.get("flow_max_std")
    pump.flow_unit_std = form.get("flow_unit_std")

    pump.cantidad_bombas = form.get("cantidad_bombas")
    pump.estado = form.get("estado")

    pump.description = form.get("description")
    pump.tag = form.get("tag")
    pump.service = form.get("service")
    pump.temperature = form.get("temperature")
    pump.materials = form.get("materials")
    pump.area = form.get("area")
    pump.location = form.get("location")

    db.commit()

    return RedirectResponse(
        url=f"/quote/{case_id}",
        status_code=303
    )


# ======================================================
# POST – ELIMINAR bomba (soft delete)
# ======================================================
@router.post("/quote/{case_id}/detected-pump/{pump_id}/delete")
async def delete_detected_pump(
    case_id: int,
    pump_id: int,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):

    pump = db.query(PumpsDetected).filter(
        PumpsDetected.id == pump_id,
        PumpsDetected.case_id == case_id
    ).first()

    if not pump:
        raise HTTPException(404, "Bomba no encontrada.")

    pump.is_active = False
    pump.updated_by = current_user_id
    pump.touch()

    db.commit()

    return RedirectResponse(
        url=f"/quote/{case_id}",
        status_code=303
    )
