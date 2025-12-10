# app/scripts/load_mroy_catalog.py
"""
Cargador de catálogo MROY (MRA1 + tablas master) desde archivos Excel a la DB.

Uso típico (desde raíz del proyecto):
    python -m app.scripts.load_mroy_catalog

Ajusta BASE_PATH si tus Excel están en otra ruta.
"""

# -------------------------------------------------------------------
# Bootstrap para poder ejecutar como script directo
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
import os
import math
import pandas as pd

from app.database.conection import SessionLocal
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
from app.models.mroy_master import (
    MroyaCapacityMaster,
    MroyaHpMaster,
    MroyaViscosidadMaster,
)

# -------------------------------------------------------------------
# Configura aquí la ruta base donde dejarás los Excel en tu proyecto
# -------------------------------------------------------------------
BASE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "database", "MRA11")
)


# -------------------------------------------------------------------
# Helpers genéricos
# -------------------------------------------------------------------
def _is_nan(val) -> bool:
    return val is None or (isinstance(val, float) and math.isnan(val))


def as_float(val):
    if _is_nan(val) or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def as_bool(val, default=False):
    if _is_nan(val) or val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    s = str(val).strip().upper()
    if s in {"TRUE", "T", "YES", "Y", "1"}:
        return True
    if s in {"FALSE", "F", "NO", "N", "0"}:
        return False
    return default


def as_str(val):
    if _is_nan(val) or val is None:
        return None
    return str(val).strip()


def _read_excel(filename: str) -> pd.DataFrame:
    """
    Lee un Excel dentro de BASE_PATH.

    - Primero intenta el nombre exacto.
    - Si no lo encuentra, lista el directorio y trata de encontrar
      un archivo que coincida de forma flexible (case-insensitive y por prefijo).
    """
    import os

    path = os.path.join(BASE_PATH, filename)

    # 1) Intento directo
    if os.path.exists(path):
        print(f"📂 Leyendo Excel (exacto): {path}")
        return pd.read_excel(path)

    # 2) Debug fuerte: mostrar qué hay realmente en la carpeta
    print("⚠ No se encontró el Excel con nombre exacto:")
    print(f"   Buscado: {path}")
    print("   Contenido real de la carpeta:")
    try:
        for f in os.listdir(BASE_PATH):
            print(f"   - {repr(f)}")
    except FileNotFoundError:
        print(f"❌ BASE_PATH no existe: {BASE_PATH}")
        raise FileNotFoundError(f"BASE_PATH no existe: {BASE_PATH}")

    # 3) Búsqueda flexible: por lower() y prefijo
    files = os.listdir(BASE_PATH)
    lower_map = {f.lower(): f for f in files}

    # a) Caso: solo difiere mayúsculas/minúsculas
    if filename.lower() in lower_map:
        real_name = lower_map[filename.lower()]
        real_path = os.path.join(BASE_PATH, real_name)
        print(f"✅ Encontrado por lower(): {real_path}")
        return pd.read_excel(real_path)

    # b) Caso: usamos prefijo sin extensión, por si hay espacios o .xls/.xlsx
    base_no_ext = os.path.splitext(filename)[0].lower()
    candidates = [
        f for f in files
        if os.path.splitext(f)[0].lower() == base_no_ext
    ]
    if len(candidates) == 1:
        real_name = candidates[0]
        real_path = os.path.join(BASE_PATH, real_name)
        print(f"✅ Encontrado por prefijo/sondeo: {real_path}")
        return pd.read_excel(real_path)

    # c) Si sigue sin encontrarse, levantamos error claro
    raise FileNotFoundError(
        f"No se encontró el Excel '{filename}' en {BASE_PATH}. "
        f"Revisa nombre exacto, mayúsculas, espacios y extensión."
    )



# -------------------------------------------------------------------
# Loaders por tabla MRA1_xx
# -------------------------------------------------------------------
def load_mroy_01_liquid(session):
    df = _read_excel("mroy_MRA1_01_liquid.xlsx")
    session.query(MroyMRA1LiquidEnd).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1LiquidEnd(
            code=int(row["code"]),
            end_material=as_str(row["end_material"]),
            material_pump=as_str(row["material_pump"]),
            head_valve=as_str(row["head_valve"]),
            diaphragm=as_str(row["diaphragm"]),
            ball_checks=as_str(row["ball_checks"]),
            contour_plate=as_str(row["contour_plate"]),
            plunger_h_applicable=as_bool(row.get("plunger_h_applicable")),
            consult_factory=as_bool(row.get("consult_factory")),
            price_usd=as_float(row.get("price_usd")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_01_liquid cargado")


def load_mroy_02_plunger(session):
    df = _read_excel("mroy_MRA1_02_plunger.xlsx")
    session.query(MroyMRA1Plunger).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1Plunger(
            code=as_str(row["code"]),
            plunger_in=as_str(row["plunger_in"]),
            material_pump=as_str(row["material_pump"]),
            plunger_mm=as_float(row["plunger_mm"]),
            max_gph=as_float(row["max_gph"]),
            max_lhr=as_float(row["max_lhr"]),
            max_pressure_psi=as_float(row["max_pressure_psi"]),
            max_pressure_bar=as_float(row["max_pressure_bar"]),
            price_usd=as_float(row["price_usd"]),
            consult_factory=as_bool(row["consult_factory"]),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_02_plunger cargado")


def load_mroy_03_gear(session):
    df = _read_excel("mroy_MRA1_03_gear.xlsx")
    session.query(MroyMRA1GearRatio).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1GearRatio(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            material_pump=as_str(row["material_pump"]),
            spm_1725rpm=as_float(row["spm_1725rpm"]),
            spm_1425rpm=as_float(row["spm_1425rpm"]),
            price_usd=as_float(row["price_usd"]),
            consult_factory=as_bool(row["consult_factory"]),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_03_gear cargado")


def load_mroy_04_motor_options(session):
    df = _read_excel("mroy_MRA1_04_motor_options.xlsx")
    session.query(MroyMRA1MotorOptions).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1MotorOptions(
            code=as_str(row["code"]),
            category=as_str(row["category"]),
            hp_kw=as_float(row.get("hp_kw")),
            enclosure=as_str(row.get("enclosure")),
            phase=as_float(row.get("phase")),
            hz=as_float(row.get("hz")),
            rpm=as_float(row.get("rpm")),
            motor_mount=as_str(row.get("motor_mount")),
            motor_part=as_str(row.get("motor_part")),
            recommended=as_bool(row.get("recommended")),
            consult_factory=as_bool(row.get("consult_factory")),
            price_usd=as_float(row.get("price_usd")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_04_motor_options cargado")


def load_mroy_05_motor_mount(session):
    df = _read_excel("mroy_MRA1_05_motor_mount.xlsx")
    session.query(MroyMRA1MotorMount).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1MotorMount(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            price_usd=as_float(row["price_usd"]),
            consult_factory=as_bool(row["consult_factory"]),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_05_motor_mount cargado")


def load_mroy_06_pipe_connections(session):
    df = _read_excel("mroy_MRA1_06_pipe_connections.xlsx")
    session.query(MroyMRA1PipeConnections).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1PipeConnections(
            code=as_str(row["code"]),
            suction_code=as_str(row["suction_code"]),
            discharge_code=as_str(row["discharge_code"]),
            metallic_applicable=as_bool(row["metallic_applicable"]),
            plastic_applicable=as_bool(row["plastic_applicable"]),
            description_metallic=as_str(row.get("description_metallic")),
            description_plastic=as_str(row.get("description_plastic")),
            price_316L_base_usd=as_float(row.get("price_316L_base_usd")),
            price_alloy20_usd=as_float(row.get("price_alloy20_usd")),
            price_PVC_base_usd=as_float(row.get("price_PVC_base_usd")),
            price_PVDF_base_usd=as_float(row.get("price_PVDF_base_usd")),
            consult_factory=as_bool(row.get("consult_factory")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_06_pipe_connections cargado")


def load_mroy_07_oring(session):
    df = _read_excel("mroy_MRA1_07_oring.xlsx")
    session.query(MroyMRA1Oring).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1Oring(
            code=as_str(row["code"]),
            material=as_str(row["material"]),
            material_pump=as_str(row["material_pump"]),
            price_usd=as_float(row["price_usd"]),
            consult_factory=as_bool(row["consult_factory"]),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_07_oring cargado")


def load_mroy_08_capacity_control(session):
    df = _read_excel("mroy_MRA1_08_Capacity_Control.xlsx")
    session.query(MroyMRA1CapacityControl).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1CapacityControl(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            internal_code=as_str(row.get("internal_code")),
            price_usd=as_float(row.get("price_usd")),
            consult_factory=as_bool(row.get("consult_factory")),
            derate_capacity=as_bool(row.get("derate_capacity")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_08_capacity_control cargado")


def load_mroy_09_diaphragm_rupture(session):
    df = _read_excel("mroy_MRA1_09_Diaphragm_Rupture.xlsx")
    session.query(MroyMRA1DiaphragmRupture).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1DiaphragmRupture(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            plunger_compatibility=as_str(row["plunger_compatibility"]),
            hi_temp_allowed=as_bool(row["hi_temp_allowed"]),
            price_usd=as_float(row["price_usd"]),
            consult_factory=as_bool(row["consult_factory"]),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_09_diaphragm_rupture cargado")


def load_mroy_10_base_options(session):
    df = _read_excel("mroy_MRA1_10_Base_Options.xlsx")
    session.query(MroyMRA1BaseOptions).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1BaseOptions(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            required_for=as_str(row.get("required_for")),
            price_usd=as_float(row["price_usd"]),
            consult_factory=as_bool(row["consult_factory"]),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_10_base_options cargado")


def load_mroy_11_code_complete(session):
    df = _read_excel("mroy_MRA1_11_Code_Complete_Identifier.xlsx")
    session.query(MroyMRA1CodeCompleteIdentifier).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1CodeCompleteIdentifier(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            required_for=as_str(row.get("required_for")),
            price_usd=as_float(row["price_usd"]),
            consult_factory=as_bool(row["consult_factory"]),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_11_code_complete_identifier cargado")


def load_mroy_12_liquid_end_extended(session):
    df = _read_excel("mroy_MRA1_12_Liquid_End_Extended_Options.xlsx")
    session.query(MroyMRA1LiquidEndExtended).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1LiquidEndExtended(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            price_usd_metallic=as_float(row.get("price_usd_metallic")),
            price_usd_pvc=as_float(row.get("price_usd_pvc")),
            price_usd_pvdf=as_float(row.get("price_usd_pvdf")),
            allow_metallic=as_bool(row.get("allow_metallic")),
            allow_pvc=as_bool(row.get("allow_pvc")),
            allow_pvdf=as_bool(row.get("allow_pvdf")),
            consult_factory=as_bool(row.get("consult_factory")),
            notes=as_str(row.get("notes")),
            restrictions=as_str(row.get("restrictions")),
        )
        session.add(obj)
    print("✔ MRA1_12_liquid_end_extended cargado")


def load_mroy_13_temperature_extended(session):
    df = _read_excel("mroy_MRA1_13_Temperature_Extended_Options.xlsx")
    session.query(MroyMRA1TemperatureExtended).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1TemperatureExtended(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            price_usd=as_float(row.get("price_usd")),
            consult_factory=as_bool(row.get("consult_factory")),
            requires_lube_option_5=as_bool(row.get("requires_lube_option_5")),
            metallic_only=as_bool(row.get("metallic_only")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_13_temperature_extended cargado")


def load_mroy_14_drive_extended(session):
    df = _read_excel("mroy_MRA1_14_Drive_Extended_Options.xlsx")
    session.query(MroyMRA1DriveExtended).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1DriveExtended(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            price_usd=as_float(row.get("price_usd")),
            consult_factory=as_bool(row.get("consult_factory")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_14_drive_extended cargado")


def load_mroy_15_motor_extended(session):
    df = _read_excel("mroy_MRA1_15_Motor_Extended_Options.xlsx")
    session.query(MroyMRA1MotorExtended).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1MotorExtended(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            price_usd=as_float(row.get("price_usd")),
            consult_factory=as_bool(row.get("consult_factory")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_15_motor_extended cargado")


def load_mroy_16_lubrication(session):
    df = _read_excel("mroy_MRA1_16_Lubrication_Options.xlsx")
    session.query(MroyMRA1LubricationOptions).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1LubricationOptions(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            price_usd=as_float(row.get("price_usd")),
            oil_type=as_str(row.get("oil_type")),
            temperature_range_F=as_str(row.get("temperature_range_F")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_16_lubrication_options cargado")


def load_mroy_17_coating_system(session):
    df = _read_excel("mroy_MRA1_17_Coating_System_Options.xlsx")
    session.query(MroyMRA1CoatingSystem).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1CoatingSystem(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            price_usd=as_float(row.get("price_usd")),
            consult_factory=as_bool(row.get("consult_factory")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_17_coating_system cargado")


def load_mroy_18_run_test(session):
    df = _read_excel("mroy_MRA1_18_Run_Test_Options.xlsx")
    session.query(MroyMRA1RunTestOptions).delete()
    for _, row in df.iterrows():
        obj = MroyMRA1RunTestOptions(
            code=as_str(row["code"]),
            description=as_str(row["description"]),
            price_usd=as_float(row.get("price_usd")),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ MRA1_18_run_test_options cargado")


# -------------------------------------------------------------------
# Loaders para tablas master
# -------------------------------------------------------------------
def load_capacity_master(session):
    df = _read_excel("mroya_capacity_master.xlsx")
    session.query(MroyaCapacityMaster).delete()
    for _, row in df.iterrows():
        obj = MroyaCapacityMaster(
            series=as_str(row["series"]),
            head_type=as_str(row["head_type"]),
            plunger_code=as_str(row["plunger_code"]),
            plunger_diameter_in=as_str(row["plunger_diameter_in"]),
            plunger_diameter_mm=as_float(row["plunger_diameter_mm"]),
            gear_ratio_code=as_str(row["gear_ratio_code"]),
            strokes60=as_float(row["strokes60"]),
            strokes50=as_float(row["strokes50"]),
            cap60_100psi_gph=as_float(row["cap60_100psi_gph"]),
            cap60_100psi_lph=as_float(row["cap60_100psi_lph"]),
            cap60_maxP_gph=as_float(row["cap60_maxP_gph"]),
            cap60_maxP_lph=as_float(row["cap60_maxP_lph"]),
            maxP_psi=as_float(row["maxP_psi"]),
            maxP_bar=as_float(row["maxP_bar"]),
            cap50_100psi_gph=as_float(row["cap50_100psi_gph"]),
            cap50_100psi_lph=as_float(row["cap50_100psi_lph"]),
            cap50_maxP_gph=as_float(row["cap50_maxP_gph"]),
            cap50_maxP_lph=as_float(row["cap50_maxP_lph"]),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ mroya_capacity_master cargado")


def load_hp_master(session):
    df = _read_excel("mroya_hp_master.xlsx")
    session.query(MroyaHpMaster).delete()
    for _, row in df.iterrows():
        obj = MroyaHpMaster(
            mroy_series=as_str(row["mroy_series"]),
            frame=as_str(row["frame"]),
            plunger_code=as_str(row["plunger_code"]),
            motor_phase=int(row["motor_phase"]),
            configuration=as_str(row["configuration"]),
            pressure_condition=as_str(row.get("pressure_condition")),
            pressure_max_psi=as_float(row.get("pressure_max_psi")),
            required_hp=as_float(row["required_hp"]),
            required_kw=as_float(row["required_kw"]),
            label_hp_kw=as_str(row["label_hp_kw"]),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ mroya_hp_master cargado")


def load_viscosidad_master(session):
    df = _read_excel("mroya_viscosidad_master.xlsx")
    session.query(MroyaViscosidadMaster).delete()
    for _, row in df.iterrows():
        obj = MroyaViscosidadMaster(
            plunger_size_in=as_str(row["plunger_size_in"]),
            plunger_size_mm=as_float(row["plunger_size_mm"]),
            plunger_code=as_str(row["plunger_code"]),
            gear_ratio=as_str(row["gear_ratio"]),
            strokes_60hz=as_float(row["strokes_60hz"]),
            strokes_50hz=as_float(row["strokes_50hz"]),
            viscosidad_con_codigo_V_cp=as_float(
                row.get("viscosidad_con_codigo_V_cp")
            ),
            viscosidad_sin_codigo_V_cp=as_float(
                row.get("viscosidad_sin_codigo_V_cp")
            ),
            notes=as_str(row.get("notes")),
        )
        session.add(obj)
    print("✔ mroya_viscosidad_master cargado")


# -------------------------------------------------------------------
# Orquestador principal
# -------------------------------------------------------------------
def load_all_mroy_catalog():
    os.makedirs(BASE_PATH, exist_ok=True)
    session = SessionLocal()
    try:
        print("▶ Iniciando carga de catálogo MROY desde:", BASE_PATH)

        # MRA1 core
        load_mroy_01_liquid(session)
        load_mroy_02_plunger(session)
        load_mroy_03_gear(session)
        load_mroy_04_motor_options(session)
        load_mroy_05_motor_mount(session)
        load_mroy_06_pipe_connections(session)
        load_mroy_07_oring(session)
        load_mroy_08_capacity_control(session)
        load_mroy_09_diaphragm_rupture(session)

        # Extended
        load_mroy_10_base_options(session)
        load_mroy_11_code_complete(session)
        load_mroy_12_liquid_end_extended(session)
        load_mroy_13_temperature_extended(session)
        load_mroy_14_drive_extended(session)
        load_mroy_15_motor_extended(session)
        load_mroy_16_lubrication(session)
        load_mroy_17_coating_system(session)
        load_mroy_18_run_test(session)

        # Masters
        load_capacity_master(session)
        load_hp_master(session)
        load_viscosidad_master(session)

        session.commit()
        print("✅ Catálogo MROY cargado correctamente en la DB.")

    except Exception as e:
        session.rollback()
        print("❌ Error cargando catálogo MROY:", repr(e))

    finally:
        session.close()


if __name__ == "__main__":
    load_all_mroy_catalog()
