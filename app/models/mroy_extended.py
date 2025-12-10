# app/models/mroy_extended.py
"""
Modelos SQLAlchemy para las tablas MRA1 de opciones extendidas:
- 10 Base Options
- 11 Code Complete Identifier
- 12 Liquid End Extended Options
- 13 Temperature Related Extended Options
- 14 Drive Extended Options
- 15 Motor Extended Options
- 16 Lubrication Options
- 17 Coating System Options
- 18 Run Test Options
"""

from sqlalchemy import Column, Integer, String, Float, Boolean, Text
from app.database.conection import Base


class MroyMRA1BaseOptions(Base):
    """
    Tabla: mroy_MRA1_10_Base_Options.xlsx
    """
    __tablename__ = "mroy_mra1_10_base_options"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # N, Y, V
    description = Column(Text, nullable=False)
    required_for = Column(Text, nullable=True)  # texto libre

    price_usd = Column(Float, nullable=False, default=0.0)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1CodeCompleteIdentifier(Base):
    """
    Tabla: mroy_MRA1_11_Code_Complete_Identifier.xlsx
    """
    __tablename__ = "mroy_mra1_11_code_complete_identifier"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # Y, N
    description = Column(Text, nullable=False)
    required_for = Column(Text, nullable=True)

    price_usd = Column(Float, nullable=False, default=0.0)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1LiquidEndExtended(Base):
    """
    Tabla: mroy_MRA1_12_Liquid_End_Extended_Options.xlsx
    """
    __tablename__ = "mroy_mra1_12_liquid_end_extended"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # N, V, S, D, G, U...
    description = Column(Text, nullable=False)

    price_usd_metallic = Column(Float, nullable=True)
    price_usd_pvc = Column(Float, nullable=True)
    price_usd_pvdf = Column(Float, nullable=True)

    allow_metallic = Column(Boolean, nullable=False, default=False)
    allow_pvc = Column(Boolean, nullable=False, default=False)
    allow_pvdf = Column(Boolean, nullable=False, default=False)

    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)
    restrictions = Column(Text, nullable=True)  # tags tipo 'only_PVC_PVDF', etc.


class MroyMRA1TemperatureExtended(Base):
    """
    Tabla: mroy_MRA1_13_Temperature_Extended_Options.xlsx
    """
    __tablename__ = "mroy_mra1_13_temperature_extended"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # N, 1, 2...
    description = Column(Text, nullable=False)

    price_usd = Column(Float, nullable=True)
    consult_factory = Column(Boolean, nullable=False, default=False)

    requires_lube_option_5 = Column(Boolean, nullable=False, default=False)
    metallic_only = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1DriveExtended(Base):
    """
    Tabla: mroy_MRA1_14_Drive_Extended_Options.xlsx
    """
    __tablename__ = "mroy_mra1_14_drive_extended"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # N, H, S, M
    description = Column(Text, nullable=False)

    price_usd = Column(Float, nullable=True)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1MotorExtended(Base):
    """
    Tabla: mroy_MRA1_15_Motor_Extended_Options.xlsx
    """
    __tablename__ = "mroy_mra1_15_motor_extended"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # N, Y, V, C
    description = Column(Text, nullable=False)

    price_usd = Column(Float, nullable=True)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1LubricationOptions(Base):
    """
    Tabla: mroy_MRA1_16_Lubrication_Options.xlsx
    """
    __tablename__ = "mroy_mra1_16_lubrication_options"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # N, 3, 4, 5, 9
    description = Column(Text, nullable=False)

    price_usd = Column(Float, nullable=False, default=0.0)
    oil_type = Column(String(100), nullable=True)
    temperature_range_F = Column(String(50), nullable=True)

    notes = Column(Text, nullable=True)


class MroyMRA1CoatingSystem(Base):
    """
    Tabla: mroy_MRA1_17_Coating_System_Options.xlsx
    """
    __tablename__ = "mroy_mra1_17_coating_system"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # N, B, C, D
    description = Column(Text, nullable=False)

    price_usd = Column(Float, nullable=True)
    consult_factory = Column(Boolean, nullable=False, default=False)

    notes = Column(Text, nullable=True)


class MroyMRA1RunTestOptions(Base):
    """
    Tabla: mroy_MRA1_18_Run_Test_Options.xlsx
    """
    __tablename__ = "mroy_mra1_18_run_test_options"

    id = Column(Integer, primary_key=True, index=True)

    code = Column(String(5), nullable=False)  # N, A, B, C, D, E, F, G...
    description = Column(Text, nullable=False)

    price_usd = Column(Float, nullable=False, default=0.0)
    notes = Column(Text, nullable=True)
