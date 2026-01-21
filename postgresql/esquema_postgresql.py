import psycopg2
from psycopg2 import sql

def crear_esquema():
    """Crea el esquema de base de datos en PostgreSQL"""
    
    # Configuración de conexión
    conexion_config = {
        "host": "localhost",
        "database": "benchmark_db",
        "user": "benchmark_user",
        "password": "benchmark123",
        "port": "5432"
    }
    
    try:
        # Conectar a PostgreSQL
        conexion = psycopg2.connect(**conexion_config)
        cursor = conexion.cursor()
        
        print("Conectado a PostgreSQL")
        
        # 1. Crear tabla Usuario
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usuario (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                pais VARCHAR(50),
                ciudad VARCHAR(50),
                fecha_registro DATE
            )
        """)
        print("Tabla 'usuario' creada/verificada")
        
        # 2. Crear tabla Producto
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS producto (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(200) NOT NULL,
                categoria VARCHAR(50),
                precio DECIMAL(10,2) CHECK (precio > 0),
                marca VARCHAR(50)
            )
        """)
        print("Tabla 'producto' creada/verificada")
        
        # 3. Crear tabla Pedido
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pedido (
                id SERIAL PRIMARY KEY,
                fecha TIMESTAMP NOT NULL,
                total DECIMAL(10,2) DEFAULT 0.00,
                estado VARCHAR(20) DEFAULT 'pendiente',
                usuario_id INTEGER REFERENCES usuario(id) ON DELETE CASCADE
            )
        """)
        print("Tabla 'pedido' creada/verificada")
        
        # 4. Crear tabla Detalle_Pedido (relación muchos a muchos)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS detalle_pedido (
                pedido_id INTEGER REFERENCES pedido(id) ON DELETE CASCADE,
                producto_id INTEGER REFERENCES producto(id) ON DELETE CASCADE,
                cantidad INTEGER CHECK (cantidad > 0),
                precio_unitario DECIMAL(10,2),
                subtotal DECIMAL(10,2),
                PRIMARY KEY (pedido_id, producto_id)
            )
        """)
        print("Tabla 'detalle_pedido' creada/verificada")
        
        # 5. Crear índices para optimización
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuario_pais ON usuario(pais)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuario_fecha_reg ON usuario(fecha_registro)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pedido_fecha ON pedido(fecha)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pedido_usuario ON pedido(usuario_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_detalle_pedido ON detalle_pedido(pedido_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_detalle_producto ON detalle_pedido(producto_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_producto_categoria ON producto(categoria)")
        
        print("Índices creados/verificados")
        
        # Confirmar cambios
        conexion.commit()
        print("\nEsquema creado exitosamente!")
        
    except Exception as e:
        print(f"Error al crear esquema: {e}")
        if 'conexion' in locals():
            conexion.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conexion' in locals():
            conexion.close()
            print("Conexión cerrada")

if __name__ == "__main__":
    crear_esquema()