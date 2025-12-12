# scripts/create_all_tables.py
# Ejecuta:  python -m scripts.create_all_tables
# (o con bootstrap como arriba si prefieres ejecutarlo directo)

# --- bootstrap opcional para ejecutar SIN -m ---
if __name__ == "__main__" and __package__ is None:
    import os, sys
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    ROOT2 = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
    if ROOT2 not in sys.path:
        sys.path.insert(0, ROOT2)
# ------------------------------------------------

from app.database.conection import Base, engine

# ===============================
# IMPORTAR MODELOS EXISTENTES
# ===============================
from app.models.user import User
from app.models.cases import Case
from app.models.documents import Document
from app.models.products import Product
from app.models.customers import Customer
from app.models.delivery_terms import DeliveryTerm
from app.models.quotes import Quote
from app.models.quote_items import QuoteItem
from app.models.pumps_detected import PumpsDetected
from app.models.mroy_selected_pump import MroySelectedPump

# ===============================
# IMPORTAR NUEVOS MODELOS MROY
# ===============================

# --- Master Tables ---
from app.models.mroy_master import (
    MroyaCapacityMaster,
    MroyaHpMaster,
    MroyaViscosidadMaster,
)

# --- MRA1 Main Tables ---
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

# --- MRA1 Extended Tables ---
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


def main():
    print("⏳ Creando/verificando todas las tablas del sistema AutoSelectX...")

    # CREATE ALL TABLES (idempotente)
    Base.metadata.create_all(bind=engine)

    print("✅ Todas las tablas han sido creadas o verificadas con éxito.")
    print("Incluye: Users, Cases, PumpsDetected, Master MROY y todas las MRA1.")


if __name__ == "__main__":
    main()

