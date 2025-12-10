from database.conection import engine
from models.user import Base

# Crear todas las tablas definidas en los modelos
print("🛠️ Creando tablas en la base de datos...")
Base.metadata.create_all(bind=engine)
print("✅ Tablas creadas exitosamente.")
