# app/scripts/seed_defaults.py

from datetime import datetime, UTC
from app.database.conection import SessionLocal

# 🔒 IMPORTAR TODO lo que tenga relationships con strings
from app.models.user import User  # noqa
from app.models.mroy_selected_pump import MroySelectedPump  # noqa
from app.models.pumps_detected import PumpsDetected  # noqa
from app.models.cases import Case  # noqa
from app.models.quotes import Quote  # noqa

# 🎯 Modelos que sí se usan
from app.models.customers import Customer
from app.models.delivery_terms import DeliveryTerm


def seed_customer(db) -> Customer:
    existing = (
        db.query(Customer)
        .filter(Customer.is_active.is_(True))
        .order_by(Customer.id.asc())
        .first()
    )
    if existing:
        print(f"✅ Customer activo ya existe: id={existing.id} name={existing.name}")
        return existing

    c = Customer(
        name="Cliente Genérico",
        nit=None,
        contact_name="Contacto",
        email="cliente@demo.com",
        phone="0000000",
        country="Colombia",
        city="Cali",
        address="N/A",
        notes="Seed default customer",
        is_active=True,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    db.add(c)
    db.commit()
    db.refresh(c)
    print(f"✅ Customer creado: id={c.id} name={c.name}")
    return c


def seed_delivery_term(db) -> DeliveryTerm:
    existing = (
        db.query(DeliveryTerm)
        .filter(DeliveryTerm.active.is_(True))
        .order_by(DeliveryTerm.id.asc())
        .first()
    )
    if existing:
        print(f"✅ DeliveryTerm activo ya existe: id={existing.id} incoterm={existing.incoterm}")
        return existing

    t = DeliveryTerm(
        incoterm="EXW",
        place="Cali, Colombia",
        lead_time_days=30,
        validity_days=30,
        warranty_months=12,
        shipping_mode="Terrestre",
        notes="Seed default delivery term",
        active=True,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    db.add(t)
    db.commit()
    db.refresh(t)
    print(f"✅ DeliveryTerm creado: id={t.id} incoterm={t.incoterm}")
    return t


def main():
    db = SessionLocal()
    try:
        seed_customer(db)
        seed_delivery_term(db)
        print("🎉 Seed defaults OK.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
