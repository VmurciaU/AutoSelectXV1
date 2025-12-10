import psycopg2

DB_NAME = "autoselectx"
DB_USER = "postgres"
DB_PASSWORD = "root"  # Reemplaza esto con tu contraseña real
DB_HOST = "localhost"
DB_PORT = "5433"

try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS mroy_bombas (
        id SERIAL PRIMARY KEY,
        codigo_parte TEXT NOT NULL,
        modelo TEXT,
        caudal_gph REAL,
        presion_psi INTEGER,
        material_cabezal TEXT,
        tipo_motor TEXT,
        voltaje TEXT,
        precio_usd REAL
    );
    """
    cursor.execute(create_table_query)
    print("✅ Tabla 'mroy_bombas' creada o ya existe.")

except Exception as e:
    print("❌ Error al conectar o crear la tabla:", e)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("🔌 Conexión cerrada.")
