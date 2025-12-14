"""
app.sripts.select_mroy_pump.py

Selector maestro de bomba MROY a partir de una bomba detectada en la DB.

Flujo:
  1) Recibe el id de la bomba detectada (tabla pumps_detected).
  2) Lee requisitos (flow, presión, viscosidad, material, temperatura...).
  3) Busca una combinación en mroya_capacity_master que cumpla:
       - head_type (metallic/plastic)
       - caudal requerido (GPH)
       - presión requerida (psi)
  4) Con la combinación elegida (series, plunger_code, gear_ratio_code):
       - REGLA 01: Liquid End (tabla MRA1_01)
       - REGLA 02: Plunger  (tabla MRA1_02)
       - REGLA 03: Gear Ratio (tabla MRA1_03)
  5) Verifica viscosidad máxima en mroya_viscosidad_master.
  6) Arma segmentos 01, 02, 03 y un código compacto de la bomba.
  7) Devuelve un dict descriptivo (sin guardar todavía en DB).

Uso típico (desde raíz del proyecto):
    python -m app.scripts.select_mroy_pump 1
"""

# -------------------------------------------------------------------
# Bootstrap para ejecutar como script directo
# -------------------------------------------------------------------
if __name__ == "__main__" and __package__ is None:
    import os
    import sys

    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)

# -------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------
import math
from typing import Dict, Any, Optional

from app.database.conection import SessionLocal



# CORE 01–09
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

# EXTENDED 10–19
from app.models.mroy_extended import (
    MroyMRA1BaseOptions,              # 10
    MroyMRA1CodeCompleteIdentifier,   # 11
    MroyMRA1LiquidEndExtended,        # 12
    MroyMRA1TemperatureExtended,      # 13
    MroyMRA1DriveExtended,            # 14
    MroyMRA1MotorExtended,            # 15
    MroyMRA1LubricationOptions,       # 16
    MroyMRA1CoatingSystem,            # 17
    MroyMRA1BaseOptions18,            # 18
    MroyMRA1RunTestOptions,           # 19
)



from app.models.mroy_master import (
    MroyaCapacityMaster,
    MroyaViscosidadMaster,
)
from app.models.pumps_detected import PumpsDetected

from app.models.mroy_selected_pump import MroySelectedPump  # 👈 NUEVO

# Solo para que SQLAlchemy registre bien relaciones (no se usan directo aquí)
from app.models.cases import Case      # noqa: F401
from app.models.user import User       # noqa: F401

from sqlalchemy.orm import Session



# -------------------------------------------------------------------
# Helpers genéricos
# -------------------------------------------------------------------
def _is_nan(val) -> bool:
    return val is None or (isinstance(val, float) and math.isnan(val))


def as_float(val, default: Optional[float] = None) -> Optional[float]:
    if _is_nan(val) or val == "":
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _material_to_head_type(material: str) -> str:
    """
    Normaliza el material EXACTAMENTE a los valores usados en DB:
        - metallic
        - plastic
    """
    if not material:
        return "metallic"

    m = material.upper()

    # Metales típicos → metallic
    if any(x in m for x in ["SS", "316", "ALLOY", "HAST", "DUPLEX", "TITANIUM"]):
        return "metallic"

    # Plásticos típicos → plastic
    if any(x in m for x in ["PVC", "PVDF", "PP", "CPVC"]):
        return "plastic"

    # fallback → metallic
    return "metallic"


def _flow_to_gph(flow_value, unit: str) -> float:
    """
    Convierte el caudal a GPH usando la unidad estándar.

    unit esperado: 'GPH' o 'LPH' (o similar).
    """
    if flow_value is None:
        return 0.0

    unit = (unit or "").upper().strip()
    f = float(flow_value)

    if unit == "GPH":
        return f
    if unit in {"LPH", "L/H", "LHR"}:
        # 1 US gal = 3.78541 L
        return f / 3.78541

    # Si la unidad es desconocida, devolvemos tal cual
    return f


def _lph_from_gph(gph: Optional[float]) -> Optional[float]:
    """Convierte GPH a LPH (solo si viene un número)."""
    if gph is None:
        return None
    return gph * 3.78541

def _as_price(val) -> float:
    try:
        if val is None:
            return 0.0
        return float(val)
    except Exception:
        return 0.0



def _get_price_usd(row) -> float:
    return _as_price(getattr(row, "price_usd", None))

def _q_by_code(db: Session, Model, code: str):
    return db.query(Model).filter(Model.code == code)

def _first_or_none(q):
    try:
        return q.first()
    except Exception:
        return None



def _price_pipe_connections(row, end_material: str, head_type: str) -> float:
    if not row:
        return 0.0
    m = (end_material or "").upper()
    if head_type == "metallic":
        if "ALLOY" in m or "20" in m:
            return _as_price(getattr(row, "price_alloy20_usd", None))
        return _as_price(getattr(row, "price_316L_base_usd", None))
    # plastic
    if "PVDF" in m:
        return _as_price(getattr(row, "price_PVDF_base_usd", None))
    return _as_price(getattr(row, "price_PVC_base_usd", None))


def _price_liquid_end_extended(row, end_material: str, head_type: str) -> float:
    if not row:
        return 0.0
    m = (end_material or "").upper()
    if head_type == "metallic":
        return _as_price(getattr(row, "price_usd_metallic", None))
    if "PVDF" in m:
        return _as_price(getattr(row, "price_usd_pvdf", None))
    return _as_price(getattr(row, "price_usd_pvc", None))



# -------------------------------------------------------------------
# Excepción específica del selector
# -------------------------------------------------------------------
class SelectionError(Exception):
    pass


# -------------------------------------------------------------------
# Selecciones por REGLA 01, 02, 03 y viscosidad
# -------------------------------------------------------------------
def select_liquid_end(
    session,
    *,
    end_material: str,
    requires_plunger_h: bool,
    ctx: Dict[str, Any],
) -> MroyMRA1LiquidEnd:
    """
    REGLA 01 — Selección del Liquid End (Tabla MRA1_01).

    - WHERE end_material = <material>
    - Si requiere plunger H → plunger_h_applicable = TRUE
    - Si consult_factory = TRUE → marcar consult_factory en el contexto.
    """
    q = session.query(MroyMRA1LiquidEnd).filter(
        MroyMRA1LiquidEnd.end_material == end_material
    )

    if requires_plunger_h:
        q = q.filter(MroyMRA1LiquidEnd.plunger_h_applicable.is_(True))
    else:
        # Si no requiere H, preferimos donde plunger_h_applicable es False si existe
        q = q.filter(MroyMRA1LiquidEnd.plunger_h_applicable.is_(False))

    le = q.first()
    if not le:
        raise SelectionError(
            f"[REGLA 01] No se encontró Liquid End para material '{end_material}' "
            f"con plunger_h_applicable={requires_plunger_h}"
        )

    if le.consult_factory:
        ctx["consult_factory"] = True
        ctx["errors"].append(
            "[REGLA 01] Liquid End requiere Consult Factory (consult_factory = TRUE)."
        )

    return le


def select_plunger(
    session,
    *,
    plunger_code: str,
    head_type: str,
    required_pressure_psi: float,
    ctx: Dict[str, Any],
) -> MroyMRA1Plunger:
    """
    REGLA 02 — Selección del Plunger (Tabla MRA1_02).

    NOTA IMPORTANTE:
    La tabla de plunger NO tiene columna material_pump.
    Por lo tanto, la selección es SOLO por plunger_code.
    """
    
    pl = (
        session.query(MroyMRA1Plunger)
        .filter(
            MroyMRA1Plunger.code == plunger_code,
            MroyMRA1Plunger.material_pump == head_type,
        )
        .first()
    )



    if not pl:
        raise SelectionError(
            f"[REGLA 02] No se encontró Plunger code='{plunger_code}'."
        )

    if required_pressure_psi and pl.max_pressure_psi is not None:
        if required_pressure_psi > pl.max_pressure_psi:
            raise SelectionError(
                f"[REGLA 02] Presión requerida {required_pressure_psi} psi excede "
                f"max_pressure_psi={pl.max_pressure_psi} del plunger {plunger_code}"
            )

    if pl.consult_factory:
        ctx["consult_factory"] = True
        ctx["errors"].append(
            f"[REGLA 02] Plunger {plunger_code} tiene consult_factory=TRUE."
        )

    return pl


def select_gear_ratio(
    session,
    *,
    gear_code: str,
    head_type: str,
    ctx: Dict[str, Any],
) -> MroyMRA1GearRatio:
    """
    REGLA 03 — Selección del Gear Ratio (Tabla MRA1_03).

    - WHERE code = <gear_ratio_code> AND material_pump = <metallic|plastic>
    - Validar incompatibilidades específicas (ratio 10 en plásticos, etc.).
    - Si consult_factory = TRUE → marcar Consult Factory.
    """
    gr = (
        session.query(MroyMRA1GearRatio)
        .filter(
            MroyMRA1GearRatio.code == gear_code,
            MroyMRA1GearRatio.material_pump == head_type,
        )
        .first()
    )

    if not gr:
        raise SelectionError(
            f"[REGLA 03] No se encontró Gear Ratio code='{gear_code}' "
            f"para head_type='{head_type}'"
        )

    code_clean = (gr.code or "").strip()

    # Regla: ratio 10 no disponible para plásticos
    if head_type == "plastic" and code_clean.startswith("10"):
        raise SelectionError(
            f"[REGLA 03] Gear Ratio {gr.code} no está permitido para bombas plásticas."
        )

    # Regla: ratio 8 → advertir que el motor debe ser 1425 rpm (lógica de motor vendrá después)
    if code_clean.startswith("8"):
        ctx["warnings"].append(
            "[REGLA 03] Gear Ratio 8/08 seleccionado: validar que el motor sea 1425 rpm (50 Hz)."
        )

    if gr.consult_factory:
        ctx["consult_factory"] = True
        ctx["errors"].append(
            f"[REGLA 03] Gear Ratio {gr.code} tiene consult_factory=TRUE (requiere fábrica)."
        )

    return gr


def check_viscosity_limit(
    session,
    *,
    plunger_code: str,
    gear_ratio_code: str,
    viscosity_cp: Optional[float],
    ctx: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Verifica la viscosidad máxima permitida usando mroya_viscosidad_master.

    - Busca fila con plunger_code + gear_ratio.
    - Compara viscosidad requerida contra:
        * viscosidad_sin_codigo_V_cp
        * viscosidad_con_codigo_V_cp
    """
    if viscosity_cp is None:
        ctx["warnings"].append(
            "[VISCOSIDAD] La viscosidad no está definida en la bomba detectada; "
            "no se valida límite de viscosidad."
        )
        return None

    row = (
        session.query(MroyaViscosidadMaster)
        .filter(
            MroyaViscosidadMaster.plunger_code == plunger_code,
            MroyaViscosidadMaster.gear_ratio == str(gear_ratio_code),
        )
        .first()
    )

    if not row:
        ctx["warnings"].append(
            f"[VISCOSIDAD] No se encontró registro en mroya_viscosidad_master para "
            f"plunger_code={plunger_code}, gear_ratio={gear_ratio_code}."
        )
        return None

    limit_no_V = row.viscosidad_sin_codigo_V_cp
    limit_with_V = row.viscosidad_con_codigo_V_cp

    if limit_no_V is not None and viscosity_cp > limit_no_V:
        ctx["warnings"].append(
            f"[VISCOSIDAD] La viscosidad requerida ({viscosity_cp} cP) supera el "
            f"límite sin V ({limit_no_V} cP) para plunger {plunger_code}, "
            f"gear {gear_ratio_code}. Podría requerir código V (High Viscosity)."
        )

    if limit_with_V is not None and viscosity_cp > limit_with_V:
        raise SelectionError(
            f"[VISCOSIDAD] La viscosidad requerida ({viscosity_cp} cP) supera incluso "
            f"el límite con código V ({limit_with_V} cP). Configuración no permitida."
        )

    return {
        "limit_without_V_cp": limit_no_V,
        "limit_with_V_cp": limit_with_V,
        "strokes_60hz": row.strokes_60hz,
        "strokes_50hz": row.strokes_50hz,
    }


# -------------------------------------------------------------------
# Selección en mroya_capacity_master
# -------------------------------------------------------------------
def find_capacity_candidate(
    session,
    *,
    head_type: str,
    required_flow_gph: float,
    required_pressure_psi: float,
    ctx: Dict[str, Any],
) -> MroyaCapacityMaster:
    """
    Busca la primera combinación en mroya_capacity_master que cumpla:
      - head_type
      - capacidad suficiente según la presión requerida
      - maxP_psi >= presión requerida
    """
    pressure = required_pressure_psi or 0.0

    # Elegimos columna de capacidad adecuada
    if pressure <= 100.0:
        flow_col = MroyaCapacityMaster.cap60_100psi_gph
    else:
        flow_col = MroyaCapacityMaster.cap60_maxP_gph
        

    head_candidates = [head_type]
    if head_type == "metallic":
        head_candidates.append("metal")
    elif head_type == "metal":
        head_candidates.append("metallic")



    q = (
        session.query(MroyaCapacityMaster)
        .filter(
            MroyaCapacityMaster.head_type.in_(head_candidates),
            flow_col >= required_flow_gph,
            MroyaCapacityMaster.maxP_psi >= pressure,
        )

    )

    cap_row = q.first()
    if not cap_row:
        raise SelectionError(
            f"[CAPACITY MASTER] No se encontró configuración que cumpla "
            f"flow >= {required_flow_gph} GPH y presión >= {required_pressure_psi} psi "
            f"para head_type='{head_type}'."
        )

    print("\n🔎 DEBUG – Capacity seleccionada")
    print("series:", cap_row.series)
    print("plunger_code:", cap_row.plunger_code)
    print("gear_ratio_code:", cap_row.gear_ratio_code)
    print("cap60_100psi_gph:", cap_row.cap60_100psi_gph)
    print("cap60_maxP_gph:", cap_row.cap60_maxP_gph)
    print("maxP_psi:", cap_row.maxP_psi)
    print()

    return cap_row


# -------------------------------------------------------------------
# Construcción de segmentos 01, 02, 03 y full_code
# -------------------------------------------------------------------
def build_code_segments(
    *,
    series: str,
    liquid_end: MroyMRA1LiquidEnd,
    plunger: MroyMRA1Plunger,
    gear_ratio: MroyMRA1GearRatio,
) -> Dict[str, Any]:
    """
    Arma los segmentos 01, 02, 03 y un código compacto de la bomba.
    """
    seg01 = str(liquid_end.code)
    seg02 = plunger.code
    seg03 = gear_ratio.code

    full_code = f"{series}-{seg01}{seg02}{seg03}"

    return {
        "series": series,
        "segments": {
            "01": {
                "label": "Liquid End",
                "code": seg01,
            },
            "02": {
                "label": "Plunger",
                "code": seg02,
            },
            "03": {
                "label": "Gear Ratio",
                "code": seg03,
            },
        },
        "full_code": full_code,
    }


def normalize_end_material(raw: str | None) -> str:
    """
    Devuelve un string que sí matchee con MroyMRA1LiquidEnd.end_material.
    Ajusta aquí según tus valores reales en la tabla MRA1_01.
    """
    if not raw:
        return "316L SS"

    s = str(raw).strip().lower()

    # 316 / inox
    if "316" in s or "inox" in s or "stainless" in s:
        return "316L SS"

    # plásticos típicos
    if "pvdf" in s:
        return "PVDF"
    if "pvc" in s:
        return "PVC"
    if "cpvc" in s:
        return "CPVC"
    if "pp" in s or "polypropylene" in s:
        return "PP"

    # fallback: usa el valor original limpio
    return str(raw).strip()


# -------------------------------------------------------------------
# Selección principal de bomba por ID (pumps_detected.id)
# -------------------------------------------------------------------
def select_mroy_pump_by_id(pump_id: int) -> Dict[str, Any]:
    """
    Punto de entrada principal:

      1) Lee bomba detectada desde DB (tabla pumps_detected).
      2) Determina material/head_type, flow, presión, viscosidad.
      3) Busca una combinación en mroya_capacity_master.
      4) Aplica REGLA 01 (Liquid End - 01), REGLA 02 (Plunger - 02), REGLA 03 (Gear - 03).
      5) Verifica viscosidad máxima.
      6) Arma segmentos 01, 02, 03 y un full_code.
      7) Devuelve un dict descriptivo (sin guardar todavía en DB).
    """
    session = SessionLocal()
    ctx: Dict[str, Any] = {
        "warnings": [],
        "errors": [],
        "consult_factory": False,
    }

    try:
        pump = session.get(PumpsDetected, pump_id)
        if not pump:
            raise SelectionError(f"No existe bomba detectada con id={pump_id}")

        print("\n🔍 DEBUG — Bomba detectada cargada desde DB")
        print("id:", pump.id)
        print("fluid:", pump.fluid)
        print("viscosity:", pump.viscosity)
        print("discharge_pressure:", pump.discharge_pressure)
        print("discharge_pressure_std:", pump.discharge_pressure_std)
        print("flow_min:", pump.flow_min)
        print("flow_nominal:", pump.flow_nominal)
        print("flow_max:", pump.flow_max)
        print("flow_max_std:", pump.flow_max_std)
        print("flow_unit_std:", pump.flow_unit_std)
        print("materials:", pump.materials)
        print("head_type (parsed):", _material_to_head_type(pump.materials or ""))
        print()

        if hasattr(pump, "is_active") and pump.is_active is False:
            ctx["warnings"].append("La bomba detectada está marcada como inactiva.")

        # -------------------------------
        # 1) Extraer requisitos básicos
        # -------------------------------
        material_raw = (pump.materials or "").strip()
        end_material = normalize_end_material(material_raw)   # ✅ para REGLA 01
        head_type = _material_to_head_type(end_material)
        
        required_pressure_psi = as_float(
            pump.discharge_pressure_std
            if pump.discharge_pressure_std is not None
            else pump.discharge_pressure
        ) or 0.0

        # Flow representativo: max > nominal > min > std
        flow_unit = pump.flow_unit_std or "GPH"
        flow_value = (
            pump.flow_max
            if pump.flow_max is not None
            else (
                pump.flow_nominal
                if pump.flow_nominal is not None
                else (
                    pump.flow_min
                    if pump.flow_min is not None
                    else pump.flow_max_std
                )
            )
        )
        required_flow_gph = _flow_to_gph(flow_value, flow_unit)

        viscosity_cp = as_float(pump.viscosity, None)
        temperature_f = as_float(pump.temperature, None)

        # -------------------------------
        # 2) Seleccionar combinación en capacity_master
        # -------------------------------
        print("🔍 DEBUG — Valores usados para selección")
        print("required_flow_gph:", required_flow_gph)
        print("required_pressure_psi:", required_pressure_psi)
        print("head_type:", head_type)
        print()

        cap_row = find_capacity_candidate(
            session,
            head_type=head_type,
            required_flow_gph=required_flow_gph,
            required_pressure_psi=required_pressure_psi,
            ctx=ctx,
        )

        series = cap_row.series
        plunger_code = cap_row.plunger_code
        gear_ratio_code = cap_row.gear_ratio_code

        requires_plunger_h = plunger_code == "H"

        # -------------------------------
        # 3) REGLA 01 — Liquid End (01)
        # -------------------------------
        liquid_end = select_liquid_end(
            session,
            end_material=end_material,  # ✅ ahora sí matchea DB
            requires_plunger_h=requires_plunger_h,
            ctx=ctx,
        )
        
        # -------------------------------
        # 4) REGLA 02 — Plunger (02)
        # -------------------------------
        plunger = select_plunger(
            session,
            plunger_code=plunger_code,
            head_type=head_type,
            required_pressure_psi=required_pressure_psi,
            ctx=ctx,
        )

        # -------------------------------
        # 5) REGLA 03 — Gear Ratio (03)
        # -------------------------------
        gear_ratio = select_gear_ratio(
            session,
            gear_code=str(gear_ratio_code),
            head_type=head_type,
            ctx=ctx,
        )

        # -------------------------------
        # 6) Verificar viscosidad máxima
        # -------------------------------
        viscosity_info = check_viscosity_limit(
            session,
            plunger_code=plunger_code,
            gear_ratio_code=str(gear_ratio_code),
            viscosity_cp=viscosity_cp,
            ctx=ctx,
        )

        # -------------------------------
        # 7) Construir segmentos y resultado
        # -------------------------------
        code_info = build_code_segments(
            series=series,
            liquid_end=liquid_end,
            plunger=plunger,
            gear_ratio=gear_ratio,
        )

        result: Dict[str, Any] = {
            "pump_id": pump_id,
            "case_id": pump.case_id,
            "tag": pump.tag,
            "service": pump.service,
            "input_requirements": {
                "fluid": pump.fluid,
                "material_raw": material_raw,
                "end_material": end_material,
                "head_type": head_type,
                "required_flow_gph": required_flow_gph,
                "required_pressure_psi": required_pressure_psi,
                "viscosity_cp": viscosity_cp,
                "temperature_f": temperature_f,
            },
            "capacity_master_row": {
                "id": cap_row.id,
                "series": cap_row.series,
                "head_type": cap_row.head_type,
                "plunger_code": cap_row.plunger_code,
                "plunger_diameter_in": cap_row.plunger_diameter_in,
                "gear_ratio_code": cap_row.gear_ratio_code,
                "strokes60": cap_row.strokes60,
                "strokes50": cap_row.strokes50,
                "cap60_maxP_gph": cap_row.cap60_maxP_gph,
                "maxP_psi": cap_row.maxP_psi,
            },
            "selected_components": {
                "liquid_end_01": {
                    "id": liquid_end.id,
                    "code": liquid_end.code,
                    "end_material": liquid_end.end_material,
                    "material_pump": liquid_end.material_pump,
                    "price_usd": liquid_end.price_usd,
                },
                "plunger_02": {
                    "id": plunger.id,
                    "code": plunger.code,
                    "plunger_in": plunger.plunger_in,
                    "material_pump": plunger.material_pump,
                    "max_pressure_psi": plunger.max_pressure_psi,
                    "price_usd": plunger.price_usd,
                },
                "gear_ratio_03": {
                    "id": gear_ratio.id,
                    "code": gear_ratio.code,
                    "description": gear_ratio.description,
                    "spm_1725rpm": gear_ratio.spm_1725rpm,
                    "spm_1425rpm": gear_ratio.spm_1425rpm,
                    "price_usd": gear_ratio.price_usd,
                },
            },
            "viscosity_limits": viscosity_info,
            "code_info": code_info,  # series, segmentos 01-02-03 y full_code
            "warnings": ctx["warnings"],
            "errors": ctx["errors"],
            "consult_factory": ctx["consult_factory"],
        }

        return result

    finally:
        session.close()

# -------------------------------------------------------------------
# Guardar/actualizar bomba seleccionada MROY en la DB (MVP)
# -------------------------------------------------------------------
def upsert_mroy_selected_pump(
    pump_id: int,
    user_id: int,
    db: Optional[Session] = None,
) -> MroySelectedPump:
    """
    Capa fina de persistencia para el MVP.

    - Lee la bomba detectada (PumpsDetected).
    - Ejecuta select_mroy_pump_by_id(pump_id) → dict con selección.
    - Crea o actualiza la fila en mroy_selected_pumps asociada a esa bomba.
    """

    own_session = False
    if db is None:
        db = SessionLocal()
        own_session = True

    try:
        # 1) Verificar que la bomba detectada exista y esté activa
        detected = (
            db.query(PumpsDetected)
            .filter(
                PumpsDetected.id == pump_id,
                PumpsDetected.is_active.is_(True),
            )
            .first()
        )
        if not detected:
            raise ValueError(f"No existe bomba detectada activa con id={pump_id}")

        # 2) Ejecutar el selector (usa su propia sesión interna)
        result = select_mroy_pump_by_id(pump_id)

        # 3) Extraer info relevante del dict
        code_info = result.get("code_info", {}) or {}
        capacity_row = result.get("capacity_master_row", {}) or {}
        components = result.get("selected_components", {}) or {}
        input_req = result.get("input_requirements", {}) or {}

        # IDs de catálogo
        capacity_master_id = capacity_row.get("id")
        liquid_end_id = components.get("liquid_end_01", {}).get("id")
        plunger_id = components.get("plunger_02", {}).get("id")
        gear_ratio_id = components.get("gear_ratio_03", {}).get("id")

        # Snapshot técnico de diseño
        design_flow_gph = input_req.get("required_flow_gph")
        design_flow_lph = _lph_from_gph(design_flow_gph)
        design_pressure_psi = input_req.get("required_pressure_psi")
        design_viscosity_cp = input_req.get("viscosity_cp")

        # Texto resumen (MVP)
        tag = detected.tag or "Sin TAG"
        fluid = detected.fluid or "-"
        service = detected.service or "-"

        if design_flow_gph is not None and design_pressure_psi is not None:
            summary_text = (
                f"{tag} – {fluid} – {service} – "
                f"Q≈{design_flow_gph:.3f} GPH, P≈{design_pressure_psi:.0f} PSI"
            )
        else:
            summary_text = f"{tag} – {fluid} – {service}"

        # 4) Buscar si ya existe selección para esta bomba
        selected = (
            db.query(MroySelectedPump)
            .filter(MroySelectedPump.detected_pump_id == pump_id)
            .first()
        )

        if selected is None:
            # Crear nueva fila
            selected = MroySelectedPump(
                case_id=detected.case_id,
                detected_pump_id=pump_id,
                created_by=user_id,
                mroy_series=code_info.get("series") or "A",
                full_code=code_info.get("full_code") or "",
            )
            db.add(selected)

        # Actualizar campos comunes
        selected.updated_by = user_id
        selected.mroy_series = code_info.get("series") or selected.mroy_series
        selected.full_code = code_info.get("full_code") or selected.full_code

        # Guardar códigos 01–03 (para modal y consistencia)
        selected.code_01 = str(result["code_info"]["segments"]["01"]["code"])
        selected.code_02 = str(result["code_info"]["segments"]["02"]["code"])
        selected.code_03 = str(result["code_info"]["segments"]["03"]["code"])



        selected.summary_text = summary_text

        # FKs de catálogo
        selected.capacity_master_id = capacity_master_id
        selected.liquid_end_id = liquid_end_id
        selected.plunger_id = plunger_id
        selected.gear_ratio_id = gear_ratio_id
        # Por ahora no guardamos viscosity_master_id específico
        # selected.viscosity_master_id = ...

        # Snapshot técnico
        selected.design_flow_gph = design_flow_gph
        selected.design_flow_lph = design_flow_lph
        selected.design_pressure_psi = design_pressure_psi
        selected.design_viscosity_cp = design_viscosity_cp

        # ✅ Precio MVP = suma 01 + 02 + 03 (también en selección automática)
        le = db.get(MroyMRA1LiquidEnd, liquid_end_id) if liquid_end_id else None
        pl = db.get(MroyMRA1Plunger, plunger_id) if plunger_id else None
        gr = db.get(MroyMRA1GearRatio, gear_ratio_id) if gear_ratio_id else None

        selected.price_total_usd = (
            _as_price(getattr(le, "price_usd", None)) +
            _as_price(getattr(pl, "price_usd", None)) +
            _as_price(getattr(gr, "price_usd", None))
        )
        selected.currency = "USD"


        selected.is_active = True
        selected.touch()

        db.commit()
        db.refresh(selected)
        return selected

    except SelectionError:
        if own_session:
            db.rollback()
        # Re-lanzamos tal cual para que la ruta pueda mostrar mensaje
        raise
    except Exception:
        if own_session:
            db.rollback()
        raise
    finally:
        if own_session:
            db.close()

def upsert_mroy_selected_pump_from_form(
    pump_id: int,
    user_id: int,
    form,
    db: Optional[Session] = None,
) -> MroySelectedPump:
    own_session = False
    if db is None:
        db = SessionLocal()
        own_session = True

    try:
        detected = (
            db.query(PumpsDetected)
            .filter(PumpsDetected.id == pump_id, PumpsDetected.is_active.is_(True))
            .first()
        )
        if not detected:
            raise ValueError(f"No existe bomba detectada activa con id={pump_id}")

        selected = (
            db.query(MroySelectedPump)
            .filter(MroySelectedPump.detected_pump_id == pump_id)
            .first()
        )

        if selected is None:
            selected = MroySelectedPump(
                case_id=detected.case_id,
                detected_pump_id=pump_id,
                created_by=user_id,
                mroy_series="MRA1",
                full_code=form.get("mroy_full_code") or "MRA1-",
            )
            db.add(selected)

        selected.updated_by = user_id
        selected.is_active = True
        selected.mroy_series = "MRA1"
        selected.full_code = form.get("mroy_full_code") or selected.full_code

        # Guardar códigos (MVP)
        for i in range(1, 20):
            key = f"code_{i:02d}"
            if hasattr(selected, key):
                setattr(selected, key, (form.get(key) or None))



        # ---------------------------------------------------
        # ✅ Resolver IDs + calcular precio (MVP FULL 01–19)
        # ---------------------------------------------------
        material_raw = (detected.materials or "").strip()
        end_material = normalize_end_material(material_raw)
        head_type = _material_to_head_type(end_material)  # metallic/plastic

        # 1) Leer códigos del form (01–19)
        codes = {}
        for i in range(1, 20):
            k = f"code_{i:02d}"
            codes[k] = (form.get(k) or "").strip() or None

        # 2) Buscar filas core 01–03 (con filtros simples)
        le_row = pl_row = gr_row = None

        # --- 01 Liquid End
        if codes["code_01"]:
            q = _q_by_code(db, MroyMRA1LiquidEnd, codes["code_01"])

            # Si existe material_pump, filtra por head_type
            if hasattr(MroyMRA1LiquidEnd, "material_pump"):
                q = q.filter(MroyMRA1LiquidEnd.material_pump == head_type)

            # Si existe end_material, filtra por end_material
            if hasattr(MroyMRA1LiquidEnd, "end_material") and end_material:
                q = q.filter(MroyMRA1LiquidEnd.end_material == end_material)

            le_row = _first_or_none(q)

        # --- 02 Plunger
        if codes["code_02"]:
            pl_row = _first_or_none(_q_by_code(db, MroyMRA1Plunger, codes["code_02"]))

        # --- 03 Gear Ratio (depende de metallic/plastic)
        if codes["code_03"]:
            q = _q_by_code(db, MroyMRA1GearRatio, codes["code_03"])
            if hasattr(MroyMRA1GearRatio, "material_pump"):
                q = q.filter(MroyMRA1GearRatio.material_pump == head_type)
            gr_row = _first_or_none(q)

        # 3) Guardar FKs base (si tu modelo selected las tiene)
        selected.liquid_end_id = le_row.id if le_row else None
        selected.plunger_id = pl_row.id if pl_row else None
        selected.gear_ratio_id = gr_row.id if gr_row else None

        # 4) Calcular precio FULL 01–19 (sumatoria best-effort)
        breakdown = {}  # para debug MVP
        total_usd = 0.0

        def add(seg: str, row):
            nonlocal total_usd
            p = _get_price_usd(row)
            breakdown[seg] = float(p)
            total_usd += p

        # --- 01–03
        add("01", le_row)
        add("02", pl_row)
        add("03", gr_row)

        # --- 04 Motor Options
        row04 = _first_or_none(_q_by_code(db, MroyMRA1MotorOptions, codes["code_04"])) if codes["code_04"] else None
        add("04", row04)

        # --- 05 Motor Mount
        row05 = _first_or_none(_q_by_code(db, MroyMRA1MotorMount, codes["code_05"])) if codes["code_05"] else None
        add("05", row05)

        # --- 06 Pipe Connections (tu UI manda "NN", "AB"...)
        row06 = None
        if codes["code_06"]:
            sc = codes["code_06"][:1]
            dc = codes["code_06"][1:2] if len(codes["code_06"]) > 1 else ""
            q = db.query(MroyMRA1PipeConnections).filter(
                MroyMRA1PipeConnections.suction_code == sc,
                MroyMRA1PipeConnections.discharge_code == dc,
            )
            if head_type == "metallic":
                q = q.filter(MroyMRA1PipeConnections.metallic_applicable.is_(True))
            else:
                q = q.filter(MroyMRA1PipeConnections.plastic_applicable.is_(True))

            row06 = _first_or_none(q)

        p06 = _price_pipe_connections(row06, end_material, head_type)
        breakdown["06"] = float(p06)
        total_usd += p06


        # --- 07 O-Ring
        row07 = None
        if codes["code_07"]:
            q = _q_by_code(db, MroyMRA1Oring, codes["code_07"])
            if hasattr(MroyMRA1Oring, "material_pump"):
                q = q.filter(MroyMRA1Oring.material_pump == head_type)
            row07 = _first_or_none(q)
        add("07", row07)

        # --- 08 Capacity Control
        row08 = _first_or_none(_q_by_code(db, MroyMRA1CapacityControl, codes["code_08"])) if codes["code_08"] else None
        add("08", row08)

        # --- 09 Rupture Detection
        row09 = _first_or_none(_q_by_code(db, MroyMRA1DiaphragmRupture, codes["code_09"])) if codes["code_09"] else None
        add("09", row09)

        # --- 10 Base Options
        row10 = _first_or_none(_q_by_code(db, MroyMRA1BaseOptions, codes["code_10"])) if codes["code_10"] else None
        add("10", row10)

        # --- 11 Code Identifier
        row11 = _first_or_none(_q_by_code(db, MroyMRA1CodeCompleteIdentifier, codes["code_11"])) if codes["code_11"] else None
        add("11", row11)

        # --- 12 Liquid End Extended
        row12 = _first_or_none(_q_by_code(db, MroyMRA1LiquidEndExtended, codes["code_12"])) if codes["code_12"] else None

        p12 = _price_liquid_end_extended(row12, end_material, head_type)
        breakdown["12"] = float(p12)
        total_usd += p12


        # --- 13 Temperature Extended
        row13 = _first_or_none(_q_by_code(db, MroyMRA1TemperatureExtended, codes["code_13"])) if codes["code_13"] else None
        add("13", row13)

        # --- 14 Drive Extended
        row14 = _first_or_none(_q_by_code(db, MroyMRA1DriveExtended, codes["code_14"])) if codes["code_14"] else None
        add("14", row14)

        # --- 15 Motor Extended
        row15 = _first_or_none(_q_by_code(db, MroyMRA1MotorExtended, codes["code_15"])) if codes["code_15"] else None
        add("15", row15)

        # --- 16 Lubrication
        row16 = _first_or_none(_q_by_code(db, MroyMRA1LubricationOptions, codes["code_16"])) if codes["code_16"] else None
        add("16", row16)

        # --- 17 Coating System
        row17 = _first_or_none(_q_by_code(db, MroyMRA1CoatingSystem, codes["code_17"])) if codes["code_17"] else None
        add("17", row17)

        # --- 18 Base Options Extra
        row18 = _first_or_none(_q_by_code(db, MroyMRA1BaseOptions18, codes["code_18"])) if codes["code_18"] else None
        add("18", row18)

        # --- 19 Run Test Options
        row19 = _first_or_none(_q_by_code(db, MroyMRA1RunTestOptions, codes["code_19"])) if codes["code_19"] else None
        add("19", row19)

        # 5) Persistir precio SIEMPRE (nunca NULL)
        selected.price_total_usd = float(total_usd)
        selected.currency = "USD"

        # ✅ Debug MVP (luego lo quitas)
        print("🧾 MROY PRICE DEBUG", {
            "pump_id": pump_id,
            "full_code": selected.full_code,
            "head_type": head_type,
            "end_material": end_material,
            "total_usd": selected.price_total_usd,
            "breakdown": breakdown,
            "codes": {k: v for k, v in codes.items() if v},
        })

        # Marcar timestamps (si tu modelo tiene touch)
        selected.touch()

        db.commit()
        db.refresh(selected)
        return selected

    except Exception:
        if own_session:
            db.rollback()
        raise
    finally:
        if own_session:
            db.close()


# -------------------------------------------------------------------
# Entry point para ejecución directa
# -------------------------------------------------------------------
def _print_pretty(result: Dict[str, Any]):
    import json
    print(json.dumps(result, indent=2, ensure_ascii=False))


def print_code_breakdown(result: Dict[str, Any]):
    """
    Imprime el desglose completo de los segmentos 01–18 usando únicamente
    los datos que YA EXISTEN en result (01–03) y placeholders para 04–19.
    """
    code_info = result.get("code_info", {})
    components = result.get("selected_components", {})

    print("\n==============================")
    print(" DESGLOSE COMPLETO DEL CÓDIGO")
    print("==============================\n")

    print("Código final:", code_info.get("full_code", "N/A"))
    print("----------------------------------------\n")

    # 01 — Liquid End
    le = components.get("liquid_end_01", {})
    print("01 — Liquid End Material")
    print(f"    Código: {le.get('code')}")
    print(f"    Material: {le.get('end_material')}")
    print(f"    Tipo bomba: {le.get('material_pump')}")
    print(f"    Precio USD: {le.get('price_usd')}\n")

    # 02 — Plunger
    pl = components.get("plunger_02", {})
    print("02 — Plunger")
    print(f"    Código: {pl.get('code')}")
    print(f"    Diámetro: {pl.get('plunger_in')}")
    print(f"    Presión máx: {pl.get('max_pressure_psi')} psi\n")

    # 03 — Gear Ratio
    gr = components.get("gear_ratio_03", {})
    print("03 — Gear Ratio")
    print(f"    Código: {gr.get('code')}")
    print(f"    Descripción: {gr.get('description')}")
    print(f"    SPM 1725rpm: {gr.get('spm_1725rpm')}")
    print(f"    SPM 1425rpm: {gr.get('spm_1425rpm')}")
    print(f"    Precio USD: {gr.get('price_usd')}\n")

    # PLACEHOLDERS PARA 04–18
    placeholders = {
        "04": "Motor Options",
        "05": "Motor Mount",
        "06": "Pipe Connections",
        "07": "O-Ring",
        "08": "Capacity Control",
        "09": "Diaphragm Rupture",
        "10": "Base Options",
        "11": "Complete Code Identifier",
        "12": "Liquid End Extended Options",
        "13": "Temperature Extended Options",
        "14": "Drive Extended Options",
        "15": "Motor Extended Options",
        "16": "Lubrication Options",
        "17": "Coating System",
        "18": "Run Test Options",
    }

    for seg, label in placeholders.items():
        print(f"{seg} — {label}: (no seleccionado todavía)")

    print("\n====================================================\n")


if __name__ == "__main__":
    import sys
    import traceback

    if len(sys.argv) < 2:
        print("Uso: python -m app.scripts.select_mroy_pump <pump_id>")
        sys.exit(1)

    pump_id = int(sys.argv[1])
    try:
        res = select_mroy_pump_by_id(pump_id)
        _print_pretty(res)         # JSON completo
        print_code_breakdown(res)  # Desglose humano 01–19
    except SelectionError as e:
        print("❌ Error de selección:", e)
    except Exception as e:
        print("❌ Error inesperado:", repr(e))
        traceback.print_exc()
