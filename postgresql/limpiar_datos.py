import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="benchmark_db",
    user="benchmark_user",
    password="benchmark123",
    port="5432"
)
cursor = conn.cursor()
cursor.execute("TRUNCATE TABLE detalle_pedido, pedido, producto, usuario RESTART IDENTITY CASCADE;")
conn.commit()
print("✅ Base de datos limpiada para nueva generación")
cursor.close()
conn.close()