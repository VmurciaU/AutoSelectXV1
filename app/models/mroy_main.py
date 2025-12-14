# app/models/mroy_main.py
"""
Modelos SQLAlchemy para las tablas MRA1 "core":
- 01 Liquid End
- 02 Plunger
- 03 Gear Ratio
- 04 Motor Options
- 05 Motor Mount
- 06 Pipe Connections
- 07 O-Ring
- 08 Capacity Control
- 09 Diaphragm Rupture Detection
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, Text
from app.database.conection import Base


class MroyMRA1LiquidEnd(Base):
    """
    Tabla: mroy_MRA1_01_liquid.xlsx
    """
    __tablename__ = "mroy_mra1_01_liquid"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(Integer, nullable=False)  # 1, 2, 5, 6, 7, 3 (Titanium)...
    end_material = Column(String(50), nullable=False)  # "316L SS", "PVC", etc.
    material_pump = Column(String(20), nullable=False)  # metallic / plastic

    head_valve = Column(String(50), nullable=False)
    diaphragm = Column(String(50), nullable=False)
    ball_checks = Column(String(50), nullable=False)
    contour_plate = Column(String(50), nullable=False)

    plunger_h_applicable = Column(Boolean, nullable=False, default=False)
    consult_factory = Column(Boolean, nullable=False, default=False)

    price_usd = Column(Float, nullable=True)  # NULL cuando es Consult Factory
    notes = Column(Text, nullable=True)


class MroyMRA1Plunger(Base):
    """
    Tabla: mroy_MRA1_02_plunger.xlsx
    """
    __tablename__ = "mroy_mra1_02_plunger"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # H, C, D, E, F
    plunger_in = Column(String(10), nullable=False)  # "3/8", "7/16", etc.
    material_pump = Column(String(20), nullable=False)  # metallic / plastic

    plunger_mm = Column(Float, nullable=False)
    max_gph = Column(Float, nullable=False)
    max_lhr = Column(Float, nullable=False)

    max_pressure_psi = Column(Float, nullable=False)
    max_pressure_bar = Column(Float, nullable=False)

    price_usd = Column(Float, nullable=False, default=0.0)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1GearRatio(Base):
    """
    Tabla: mroy_MRA1_03_gear.xlsx
    """
    __tablename__ = "mroy_mra1_03_gear"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(10), nullable=False)  # 77, 48, 24, 15, 10, 08...
    description = Column(String(100), nullable=False)

    material_pump = Column(String(20), nullable=False)  # metallic / plastic

    spm_1725rpm = Column(Float, nullable=True)
    spm_1425rpm = Column(Float, nullable=True)

    price_usd = Column(Float, nullable=False, default=0.0)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1MotorOptions(Base):
    """
    Tabla: mroy_MRA1_04_motor_options.xlsx
    """
    __tablename__ = "mroy_mra1_04_motor_options"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(10), nullable=False)  # S5, S1, S7, 5X, 1X, etc.
    category = Column(String(100), nullable=False)

    hp_kw = Column(Float, nullable=True)  # algunos vienen en blanco
    enclosure = Column(String(20), nullable=True)  # TEFC, TENV, XPFC...
    phase = Column(Integer, nullable=True)  # 1, 3
    hz = Column(Integer, nullable=True)  # 50, 60
    rpm = Column(Integer, nullable=True)  # 1425, 1725

    motor_mount = Column(String(50), nullable=True)  # NEMA 56C, IEC Frame 71...
    motor_part = Column(String(50), nullable=True)   # Part number Milton Roy

    recommended = Column(Boolean, nullable=False, default=False)
    consult_factory = Column(Boolean, nullable=False, default=False)

    price_usd = Column(Float, nullable=False, default=0.0)
    notes = Column(Text, nullable=True)


class MroyMRA1MotorMount(Base):
    """
    Tabla: mroy_MRA1_05_motor_mount.xlsx
    """
    __tablename__ = "mroy_mra1_05_motor_mount"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # C, A
    description = Column(String(200), nullable=False)

    price_usd = Column(Float, nullable=False, default=0.0)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1PipeConnections(Base):
    """
    Tabla: mroy_MRA1_06_pipe_connections.xlsx
    """
    __tablename__ = "mroy_mra1_06_pipe_connections"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # P, A, B, C, D, E, F...
    suction_code = Column(String(5), nullable=False)
    discharge_code = Column(String(5), nullable=False)

    metallic_applicable = Column(Boolean, nullable=False, default=False)
    plastic_applicable = Column(Boolean, nullable=False, default=False)

    description_metallic = Column(Text, nullable=True)
    description_plastic = Column(Text, nullable=True)

    price_316L_base_usd = Column(Float, nullable=True)
    price_alloy20_usd = Column(Float, nullable=True)
    price_PVC_base_usd = Column(Float, nullable=True)
    price_PVDF_base_usd = Column(Float, nullable=True)

    consult_factory = Column(Boolean, nullable=False, default=False)
    notes = Column(Text, nullable=True)


class MroyMRA1Oring(Base):
    """
    Tabla: mroy_MRA1_07_oring.xlsx
    """
    __tablename__ = "mroy_mra1_07_oring"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # V, E, T, etc.
    material = Column(String(50), nullable=False)  # Viton, EPDM, Teflex...
    material_pump = Column(String(20), nullable=False)  # metallic / plastic

    price_usd = Column(Float, nullable=False, default=0.0)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1CapacityControl(Base):
    """
    Tabla: mroy_MRA1_08_Capacity_Control.xlsx
    """
    __tablename__ = "mroy_mra1_08_capacity_control"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # N, S, L, W, E, ...
    description = Column(Text, nullable=False)
    internal_code = Column(String(50), nullable=True)  # Part number ACC

    price_usd = Column(Float, nullable=True)
    consult_factory = Column(Boolean, nullable=False, default=False)
    derate_capacity = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1DiaphragmRupture(Base):
    """
    Tabla: mroy_MRA1_09_Diaphragm_Rupture.xlsx
    """
    __tablename__ = "mroy_mra1_09_diaphragm_rupture"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)
    description = Column(Text, nullable=False)

    plunger_compatibility = Column(Text, nullable=False)
    hi_temp_allowed = Column(Boolean, nullable=False, default=False)

    price_usd = Column(Float, nullable=True)  # NULL permitido (no aplica)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)
