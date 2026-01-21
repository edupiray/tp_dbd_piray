import psycopg2

def otorgar_permisos():
    # Conéctate como postgres (usa TU contraseña de postgres)
    conn = psycopg2.connect(
        host="localhost",
        database="benchmark_db",
        user="postgres",
        password="postgres",  # 🔴 ¡CAMBIAR ESTO!
        port="5432"
    )
    cursor = conn.cursor()
    
    try:
        print("Otorgando permisos a benchmark_user...")
        cursor.execute("GRANT ALL ON SCHEMA public TO benchmark_user")
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO benchmark_user")
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO benchmark_user")
        conn.commit()
        print("✅ Permisos otorgados correctamente")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    otorgar_permisos()