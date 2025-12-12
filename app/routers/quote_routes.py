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
from app.models.mroy_selected_pump import MroySelectedPump

# ⬇️ MODELOS MROY (core 01–09)
from app.models.mroy_main import (
    MroyMRA1LiquidEnd,
    MroyMRA1Plunger,
    MroyMRA1GearRatio,
    MroyMRA1MotorOptions,
    MroyMRA1MotorMount,
    MroyMRA1PipeConnections,
    MroyMRA1Oring,
    MroyMRA1CapacityControl,
    MroyMRA1DiaphragmRupture,
)

# ⬇️ MODELOS MROY (extendidas 10–18)
from app.models.mroy_extended import (
    MroyMRA1BaseOptions,
    MroyMRA1CodeCompleteIdentifier,
    MroyMRA1LiquidEndExtended,
    MroyMRA1TemperatureExtended,
    MroyMRA1DriveExtended,
    MroyMRA1MotorExtended,
    MroyMRA1LubricationOptions,
    MroyMRA1CoatingSystem,
    MroyMRA1RunTestOptions,
)

# ⬇️ Servicio maestro de selección MROY (usa el script que ya creaste)
from app.scripts.select_mroy_pump import (
    upsert_mroy_selected_pump,
    SelectionError,
    select_mroy_pump_by_id,   # ⬅️ NUEVO
)

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


# ================================================
# HELPER: Catálogo MROY para el modal
# ================================================
def get_mroy_catalog_context(db: Session) -> dict:
    """
    Devuelve todas las listas de opciones que el modal MROY necesita.
    Se usa en la vista de cotización para llenar los <select>.
    """

    # Core 01–09
    liquid_end_options = (
        db.query(MroyMRA1LiquidEnd)
        .order_by(MroyMRA1LiquidEnd.code)
        .all()
    )

    plunger_options = (
        db.query(MroyMRA1Plunger)
        .order_by(MroyMRA1Plunger.code)
        .all()
    )

    gear_options = (
        db.query(MroyMRA1GearRatio)
        .order_by(MroyMRA1GearRatio.code)
        .all()
    )

    motor_options = (
        db.query(MroyMRA1MotorOptions)
        .order_by(MroyMRA1MotorOptions.code)
        .all()
    )

    motor_mount_options = (
        db.query(MroyMRA1MotorMount)
        .order_by(MroyMRA1MotorMount.code)
        .all()
    )

    pipe_connection_options = (
        db.query(MroyMRA1PipeConnections)
        .order_by(MroyMRA1PipeConnections.code)
        .all()
    )

    oring_options = (
        db.query(MroyMRA1Oring)
        .order_by(MroyMRA1Oring.code)
        .all()
    )

    capacity_control_options = (
        db.query(MroyMRA1CapacityControl)
        .order_by(MroyMRA1CapacityControl.code)
        .all()
    )

    rupture_options = (
        db.query(MroyMRA1DiaphragmRupture)
        .order_by(MroyMRA1DiaphragmRupture.code)
        .all()
    )

    # Extendidas 10–18
    base_options = (
        db.query(MroyMRA1BaseOptions)
        .order_by(MroyMRA1BaseOptions.code)
        .all()
    )

    code_identifier_options = (
        db.query(MroyMRA1CodeCompleteIdentifier)
        .order_by(MroyMRA1CodeCompleteIdentifier.code)
        .all()
    )

    liquid_end_ext_options = (
        db.query(MroyMRA1LiquidEndExtended)
        .order_by(MroyMRA1LiquidEndExtended.code)
        .all()
    )

    temperature_ext_options = (
        db.query(MroyMRA1TemperatureExtended)
        .order_by(MroyMRA1TemperatureExtended.code)
        .all()
    )

    drive_ext_options = (
        db.query(MroyMRA1DriveExtended)
        .order_by(MroyMRA1DriveExtended.code)
        .all()
    )

    motor_ext_options = (
        db.query(MroyMRA1MotorExtended)
        .order_by(MroyMRA1MotorExtended.code)
        .all()
    )

    lubrication_options = (
        db.query(MroyMRA1LubricationOptions)
        .order_by(MroyMRA1LubricationOptions.code)
        .all()
    )

    coating_options = (
        db.query(MroyMRA1CoatingSystem)
        .order_by(MroyMRA1CoatingSystem.code)
        .all()
    )

    run_test_options = (
        db.query(MroyMRA1RunTestOptions)
        .order_by(MroyMRA1RunTestOptions.code)
        .all()
    )

    # ⬇️ AQUÍ aplicamos el filtro para el UI
    liquid_end_options        = dedupe_by_code(liquid_end_options)
    plunger_options           = dedupe_by_code(plunger_options)
    gear_options              = dedupe_by_code(gear_options)
    motor_options             = dedupe_by_code(motor_options)
    motor_mount_options       = dedupe_by_code(motor_mount_options)
    pipe_connection_options   = dedupe_by_code(pipe_connection_options)
    oring_options             = dedupe_by_code(oring_options)
    capacity_control_options  = dedupe_by_code(capacity_control_options)
    rupture_options           = dedupe_by_code(rupture_options)
    base_options              = dedupe_by_code(base_options)
    code_identifier_options   = dedupe_by_code(code_identifier_options)
    liquid_end_ext_options    = dedupe_by_code(liquid_end_ext_options)
    temperature_ext_options   = dedupe_by_code(temperature_ext_options)
    drive_ext_options         = dedupe_by_code(drive_ext_options)
    motor_ext_options         = dedupe_by_code(motor_ext_options)
    lubrication_options       = dedupe_by_code(lubrication_options)
    coating_options           = dedupe_by_code(coating_options)
    run_test_options          = dedupe_by_code(run_test_options)

    return {
        "liquid_end_options": liquid_end_options,
        "plunger_options": plunger_options,
        "gear_options": gear_options,
        "motor_options": motor_options,
        "motor_mount_options": motor_mount_options,
        "pipe_connection_options": pipe_connection_options,
        "oring_options": oring_options,
        "capacity_control_options": capacity_control_options,
        "rupture_options": rupture_options,
        "base_options": base_options,
        "code_identifier_options": code_identifier_options,
        "liquid_end_ext_options": liquid_end_ext_options,
        "temperature_ext_options": temperature_ext_options,
        "drive_ext_options": drive_ext_options,
        "motor_ext_options": motor_ext_options,
        "lubrication_options": lubrication_options,
        "coating_options": coating_options,
        "run_test_options": run_test_options,
    }


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
            if "items" in parsed and isinstance(parsed["items"], list):
                return parsed["items"]
            return []

    # Si es dict simple o cualquier otra cosa, no nos arriesgamos
    if isinstance(raw, dict):
        if "detected" in raw and isinstance(raw["detected"], list):
            return raw["detected"]
        return []

    # Fallback
    return []


# -----------------------------------------------
# Helper: dejar una sola opción por código (UI)
# -----------------------------------------------
def dedupe_by_code(items):
    """
    Recibe una lista de filas con atributo .code y
    devuelve solo una (la primera) por cada código.
    Esto es SOLO para el modal (UI), la lógica de
    selección sigue usando todas las filas en la BD.
    """
    if not items:
        return []

    seen = set()
    result = []
    for item in items:
        code = getattr(item, "code", None)
        # Si no tiene code, lo dejamos pasar
        if code is None:
            result.append(item)
            continue

        if code in seen:
            # Ya mostramos este code en el combo, lo saltamos
            continue

        seen.add(code)
        result.append(item)

    return result



# -----------------------------------------------
# Helpers: conversión desde formulario HTML
# -----------------------------------------------
def to_float_or_none(value):
    """
    Convierte valores provenientes del formulario a float o None.
    Maneja:
      - None
      - "", "   "  -> None
      - "2.5", "0.1" -> float
      - ints / Decimal -> float
    Evita mandar "" a columnas Float de la BD.
    """
    if value is None:
        return None
    # Si ya viene como número (raro, pero por si acaso)
    if isinstance(value, (int, float, decimal.Decimal)):
        return float(value)
    value = str(value).strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        # Si llega basura, preferimos NULL antes que reventar
        return None


def to_int_or_none(value):
    """
    Convierte valores del formulario a int o None.
    Maneja:
      - None
      - "", "   " -> None
      - "1", "1.0" -> 1
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, (float, decimal.Decimal)):
        return int(value)
    value = str(value).strip()
    if value == "":
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def normalize_str(value):
    """
    Limpia strings opcionales:
      - None -> None
      - "   " -> None
      - "  texto  " -> "texto"
    """
    if value is None:
        return None
    value = str(value).strip()
    return value or None


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
        raise HTTPException(401, "Usuario no encontrado o sesión inválida")

    # ------------------------------------
    # 3. Cargar bombas detectadas desde BD
    # ------------------------------------
    detected_from_db = (
        db.query(PumpsDetected)
        .filter(
            PumpsDetected.case_id == case_id,
            PumpsDetected.is_active == True,
        )
        .all()
    )
    has_pumps_in_db = len(detected_from_db) > 0

    # ------------------------------------
    # 3.b Cargar bombas MROY seleccionadas desde BD
    # ------------------------------------
    selected_pumps = (
        db.query(MroySelectedPump)
        .filter(
            MroySelectedPump.case_id == case_id,
            MroySelectedPump.is_active == True,
        )
        .order_by(MroySelectedPump.id.asc())
        .all()
    )
    has_selected_pumps = len(selected_pumps) > 0
    
        # Mapa auxiliar: detected_pump_id -> MroySelectedPump
    selected_by_pump_id = {
        sp.detected_pump_id: sp for sp in selected_pumps
    }


    # ------------------------------------
    # 4. Cargar bombas detectadas del asistente (JSON / storage)
    # ------------------------------------
    raw_detected = load_requirements_from_storage(case_id)
    detected_from_json = _normalize_detected_list(raw_detected)
    
    
    
        # ------------------------------------
    # 5. Sugerencias automáticas MROY 01–03
    # ------------------------------------
    auto_mroy_suggestions = {}

    for pump in detected_from_db:
        # Solo bombas activas
        if getattr(pump, "is_active", True) is False:
            continue

        # Si ya hay bomba MROY seleccionada para esta bomba detectada, no sugerimos nada
        sp = selected_by_pump_id.get(pump.id)
        if sp and sp.full_code:
            continue

        try:
            res = select_mroy_pump_by_id(pump.id)
            code_info = (res.get("code_info") or {})
            segments = (code_info.get("segments") or {})

            auto_mroy_suggestions[pump.id] = {
                "code_01": (segments.get("01") or {}).get("code"),
                "code_02": (segments.get("02") or {}).get("code"),
                "code_03": (segments.get("03") or {}).get("code"),
            }
        except SelectionError:
            # Si falla la regla (viscosidad, presión, etc.), simplemente no damos sugerencia
            continue

    
    
    

    # ------------------------------------
    # 5. Construir JSON maestro para JS (modales)
    # ------------------------------------
    try:
        pumps_payload = {
            "detected": detected_from_json or [],
            "db": [
                (
                    lambda sp: {
                    "id": pump.id,
                    "tag": pump.tag,
                    "fluid": pump.fluid,
                    "flow_max_std": pump.flow_max_std,
                    "flow_unit_std": pump.flow_unit_std,
                    "discharge_pressure_std": pump.discharge_pressure_std,
                    "discharge_pressure": pump.discharge_pressure,
                    "cantidad_bombas": pump.cantidad_bombas,

                    # Identidad MROY
                    "mroy_full_code": sp.full_code if sp else None,

                    # 🔥 CÓDIGOS MROY DESDE BD (01–18)
                    "mroy_codes": {
                        "01": sp.code_01 if sp else None,
                        "02": sp.code_02 if sp else None,
                        "03": sp.code_03 if sp else None,
                        "04": sp.code_04 if sp else None,
                        "05": sp.code_05 if sp else None,
                        "06": sp.code_06 if sp else None,
                        "07": sp.code_07 if sp else None,
                        "08": sp.code_08 if sp else None,
                        "09": sp.code_09 if sp else None,
                        "10": sp.code_10 if sp else None,
                        "11": sp.code_11 if sp else None,
                        "12": sp.code_12 if sp else None,
                        "13": sp.code_13 if sp else None,
                        "14": sp.code_14 if sp else None,
                        "15": sp.code_15 if sp else None,
                        "16": sp.code_16 if sp else None,
                        "17": sp.code_17 if sp else None,
                        "18": sp.code_18 if sp else None,
                } if sp else None,
            }
        )(selected_by_pump_id.get(pump.id))
        for pump in detected_from_db
    ],
}


        pumps_payload_json = json.dumps(
            pumps_payload,
            ensure_ascii=False,
            default=_json_default,
        )
    except Exception as e:
        print(f"[quote_routes] ERROR serializando pumps_payload: {e}")
        pumps_payload_json = json.dumps(
            {"detected": [], "db": []},
            ensure_ascii=False,
        )

    # ------------------------------------
    # 6. Contexto render
    # ------------------------------------
    context = {
        "request": request,
        "case": case,

        # Panel JSON (asistente)
        "requirements": detected_from_json,
        "requirements_json": detected_from_json,

        # Panel Bombas detectadas en BD
        "pumps_from_db": detected_from_db,
        "has_pumps_in_db": has_pumps_in_db,

        # Panel Bombas MROY seleccionadas
        "selected_pumps": selected_pumps,
        "has_selected_pumps": has_selected_pumps,

        # JSON maestro para JS
        "pumps_payload_json": pumps_payload_json,
        
        # ⬇️ NUEVO: sugerencias automáticas para 01–03
        "auto_mroy_suggestions_json": json.dumps(
            auto_mroy_suggestions,
            ensure_ascii=False,
            default=_json_default,
        ),

        # Otros elementos del banner de cotización
        "quote_items": [],
        "selected_client": None,
        "selected_terms": None,
        "quote_notes": "",
        "user_name": current_user.nombre,
        "user_rol": current_user.rol,
    }

    # ⬇️ AÑADIR CATÁLOGO MROY PARA EL MODAL
    context.update(get_mroy_catalog_context(db))

    return templates.TemplateResponse(
        "assistant/_quote_final.html",
        context,
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
    existing = (
        db.query(PumpsDetected)
        .filter(
            PumpsDetected.case_id == case_id,
            PumpsDetected.is_active == True,
        )
        .count()
    )

    if existing > 0:
        raise HTTPException(
            400,
            "Ya existen bombas detectadas guardadas para este caso.",
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
        status_code=303,
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
    # 1. Buscar bomba activa
    pump = (
        db.query(PumpsDetected)
        .filter(
            PumpsDetected.id == pump_id,
            PumpsDetected.case_id == case_id,
            PumpsDetected.is_active == True,
        )
        .first()
    )

    if not pump:
        raise HTTPException(404, "Bomba no encontrada o inactiva.")

    # 2. Leer formulario
    form = await request.form()

    # 3. Auditoría
    pump.updated_by = current_user_id
    pump.touch()

    # 4. Actualizar campos numéricos (Float → usando helper)
    pump.viscosity = to_float_or_none(form.get("viscosity"))

    pump.discharge_pressure = to_float_or_none(form.get("discharge_pressure"))
    pump.discharge_pressure_std = to_float_or_none(
        form.get("discharge_pressure_std")
    )

    pump.flow_min = to_float_or_none(form.get("flow_min"))
    pump.flow_nominal = to_float_or_none(form.get("flow_nominal"))
    pump.flow_max = to_float_or_none(form.get("flow_max"))

    pump.flow_max_std = to_float_or_none(form.get("flow_max_std"))

    pump.temperature = to_float_or_none(form.get("temperature"))

    # 5. Actualizar cantidad_bombas (Integer, NOT NULL)
    raw_cant = form.get("cantidad_bombas")
    new_cant = to_int_or_none(raw_cant)
    if new_cant is not None and new_cant > 0:
        pump.cantidad_bombas = new_cant

    # 6. Actualizar campos de texto
    pump.fluid = normalize_str(form.get("fluid"))
    pump.flow_unit_std = normalize_str(form.get("flow_unit_std"))

    pump.description = normalize_str(form.get("description"))
    pump.tag = normalize_str(form.get("tag"))
    pump.service = normalize_str(form.get("service"))
    pump.materials = normalize_str(form.get("materials"))
    pump.area = normalize_str(form.get("area"))
    pump.location = normalize_str(form.get("location"))

    # 7. Estado (borrador / ajustada / validada)
    estado_form = normalize_str(form.get("estado"))
    if estado_form in {"borrador", "ajustada", "validada"}:
        pump.estado = estado_form

    # 8. Commit
    db.commit()

    # 9. Redirigir a GET
    return RedirectResponse(
        url=f"/quote/{case_id}",
        status_code=303,
    )


# ======================================================
# POST – Seleccionar bomba MROY para una bomba detectada
# ======================================================
@router.post("/quote/{case_id}/detected-pump/{pump_id}/select-mroy")
async def select_mroy_pump_route(
    case_id: int,
    pump_id: int,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    """
    Ejecuta el selector maestro de bomba MROY a partir de una bomba detectada:

      - Verifica que el caso exista.
      - Verifica que la bomba detectada esté activa y pertenezca al caso.
      - Llama a upsert_mroy_selected_pump(pump_id, current_user_id, db).
      - Redirige de nuevo a la vista de cotización.
    """

    # 1. Validar caso
    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        raise HTTPException(404, "Caso no encontrado")

    # 2. Validar bomba detectada activa
    pump = (
        db.query(PumpsDetected)
        .filter(
            PumpsDetected.id == pump_id,
            PumpsDetected.case_id == case_id,
            PumpsDetected.is_active == True,
        )
        .first()
    )
    if not pump:
        raise HTTPException(404, "Bomba detectada no encontrada o inactiva.")

    # 3. Ejecutar selector y persistir selección
    try:
        _selected = upsert_mroy_selected_pump(
            pump_id=pump_id,
            user_id=current_user_id,
            db=db,
        )
    except SelectionError as e:
        # Error de reglas de selección (viscosidad, presión, catálogo, etc.)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Cualquier otro error inesperado
        raise HTTPException(
            status_code=500,
            detail=f"Error seleccionando bomba MROY: {e}",
        )

    # 4. Redirigir a la vista de cotización
    return RedirectResponse(
        url=f"/quote/{case_id}",
        status_code=303,
    )



@router.post("/quote/{case_id}/detected-pump/{pump_id}/save-mroy-selection")
async def save_mroy_selection_manual(
    case_id: int,
    pump_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id),
):
    # 1) Validar caso
    case = db.query(Case).filter(Case.id == case_id).first()
    if not case:
        raise HTTPException(404, "Caso no encontrado")

    # 2) Validar bomba detectada activa
    pump = (
        db.query(PumpsDetected)
        .filter(
            PumpsDetected.id == pump_id,
            PumpsDetected.case_id == case_id,
            PumpsDetected.is_active == True,
        )
        .first()
    )
    if not pump:
        raise HTTPException(404, "Bomba detectada no encontrada o inactiva.")

    # 3) Leer formulario
    form = await request.form()

    # 4) Tomar códigos 01–18
    codes = {}
    for i in range(1, 19):
        k = f"{i:02d}"
        codes[k] = (form.get(f"code_{k}") or "").strip() or None

    # 5) Armar full_code (simple y consistente con tu preview)
    #    MRA1-{01}{02}{03}-{04..18}
    core = f"{codes['01'] or ''}{codes['02'] or ''}{codes['03'] or ''}"
    extras = "".join([codes[f"{i:02d}"] or "" for i in range(4, 19)])
    full_code = f"MRA1-{core}"
    if extras:
        full_code += f"-{extras}"

    # 6) Upsert en mroy_selected_pumps (persistir)
    selected = (
        db.query(MroySelectedPump)
        .filter(MroySelectedPump.detected_pump_id == pump_id)
        .first()
    )

    if selected is None:
        selected = MroySelectedPump(
            case_id=case_id,
            detected_pump_id=pump_id,
            created_by=current_user_id,
            mroy_series="A",  # por ahora fijo
            full_code=full_code,
        )
        db.add(selected)

    selected.updated_by = current_user_id
    selected.is_active = True
    selected.full_code = full_code

    # Guardar códigos 01–18
    selected.code_01 = codes["01"]
    selected.code_02 = codes["02"]
    selected.code_03 = codes["03"]
    selected.code_04 = codes["04"]
    selected.code_05 = codes["05"]
    selected.code_06 = codes["06"]
    selected.code_07 = codes["07"]
    selected.code_08 = codes["08"]
    selected.code_09 = codes["09"]
    selected.code_10 = codes["10"]
    selected.code_11 = codes["11"]
    selected.code_12 = codes["12"]
    selected.code_13 = codes["13"]
    selected.code_14 = codes["14"]
    selected.code_15 = codes["15"]
    selected.code_16 = codes["16"]
    selected.code_17 = codes["17"]
    selected.code_18 = codes["18"]

    selected.touch()
    db.commit()

    return RedirectResponse(
        url=f"/quote/{case_id}",
        status_code=303,
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

    pump = (
        db.query(PumpsDetected)
        .filter(
            PumpsDetected.id == pump_id,
            PumpsDetected.case_id == case_id,
            PumpsDetected.is_active == True,
        )
        .first()
    )

    if not pump:
        raise HTTPException(
            404,
            "Bomba no encontrada o ya inactiva.",
        )

    pump.is_active = False
    pump.updated_by = current_user_id
    pump.touch()

    db.commit()

    return RedirectResponse(
        url=f"/quote/{case_id}",
        status_code=303,
    )
