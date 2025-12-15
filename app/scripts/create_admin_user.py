# app/scripts/create_admin_user.py

from passlib.context import CryptContext
from sqlalchemy import text
from app.database.conection import SessionLocal

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

EMAIL = "admin@gmail.com"
PASSWORD = "123456"
NOMBRE = "Admin"
ROL = "admin"


def main():
    db = SessionLocal()
    try:
        exists = db.execute(
            text("SELECT id FROM users WHERE email = :email LIMIT 1"),
            {"email": EMAIL},
        ).fetchone()

        if exists:
            print(f"✅ Ya existe: {EMAIL} (id={exists[0]})")
            db.execute(
                text("""
                    UPDATE users
                    SET activo = TRUE,
                        rol = :rol,
                        nombre = :nombre
                    WHERE email = :email
                """),
                {"email": EMAIL, "rol": ROL, "nombre": NOMBRE},
            )
            db.commit()
            print("✅ Usuario actualizado (activo/admin).")
            return

        password_hash = pwd_context.hash(PASSWORD)

        db.execute(
            text("""
                INSERT INTO users
                (nombre, email, password_hash, rol, activo, fecha_creacion)
                VALUES (:nombre, :email, :password_hash, :rol, TRUE, NOW())
            """),
            {
                "nombre": NOMBRE,
                "email": EMAIL,
                "password_hash": password_hash,
                "rol": ROL,
            },
        )
        db.commit()
        print(f"✅ Admin creado: {EMAIL} / {PASSWORD}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
