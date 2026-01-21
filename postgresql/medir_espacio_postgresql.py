import psycopg2
import os
import sys

def medir_espacio_bd(host="localhost", database="benchmark_db", 
                    user="benchmark_user", password="benchmark123"):
    """Mide el espacio utilizado por la base de datos - VERSIÓN CORREGIDA"""
    
    try:
        # Conectar a PostgreSQL
        conexion = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port="5432"
        )
        cursor = conexion.cursor()
        
        print("Midiendo espacio en disco de PostgreSQL...")
        
        # 1. Tamaño total de la base de datos (MÉTODO CORREGIDO)
        cursor.execute("SELECT pg_database_size(current_database()) / (1024.0 * 1024.0) as size_mb")
        tamaño_total_mb = cursor.fetchone()[0]
        
        # 2. Tamaño por tabla (esto SÍ funciona con benchmark_user)
        cursor.execute("""
            SELECT 
                tablename,
                pg_total_relation_size('public.' || tablename) / (1024.0 * 1024.0) as size_mb,
                pg_relation_size('public.' || tablename) / (1024.0 * 1024.0) as table_mb,
                (pg_total_relation_size('public.' || tablename) - 
                 pg_relation_size('public.' || tablename)) / (1024.0 * 1024.0) as indexes_mb
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY size_mb DESC
        """)
        
        tablas = cursor.fetchall()
        
        # 3. Contar registros
        cursor.execute("SELECT COUNT(*) FROM usuario")
        usuarios = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM producto")
        productos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM pedido")
        pedidos = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM detalle_pedido")
        detalles = cursor.fetchone()[0]
        
        # 4. Espacio de índices adicional
        cursor.execute("""
            SELECT SUM(pg_indexes_size(c.oid)) / (1024.0 * 1024.0) as total_indexes_mb
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
        """)
        total_indexes_mb = cursor.fetchone()[0] or 0
        
        print("\n" + "="*60)
        print("INFORME DE ESPACIO - POSTGRESQL")
        print("="*60)
        
        print(f"\nESTADÍSTICAS DE REGISTROS:")
        print(f"   Usuarios: {usuarios:,}")
        print(f"   Productos: {productos:,}")
        print(f"   Pedidos: {pedidos:,}")
        print(f"   Detalles pedido: {detalles:,}")
        print(f"   Total entidades: {usuarios + productos + pedidos + detalles:,}")
        
        print(f"\nTAMAÑO TOTAL DE LA BD: {tamaño_total_mb:.2f} MB")
        
        print(f"\nDISTRIBUCIÓN POR TABLA:")
        print("-"*65)
        print(f"{'Tabla':<20} {'Total (MB)':<12} {'Datos (MB)':<12} {'Índices (MB)':<12}")
        print("-"*65)
        
        total_tablas_mb = 0
        for tabla in tablas:
            tablename, size_mb, table_mb, indexes_mb = tabla
            print(f"{tablename:<20} {size_mb:<12.2f} {table_mb:<12.2f} {indexes_mb:<12.2f}")
            total_tablas_mb += size_mb
        
        print("-"*65)
        print(f"{'TOTAL':<20} {total_tablas_mb:<12.2f}")
        
        # Calcular eficiencia
        if usuarios > 0:
            kb_por_usuario = (tamaño_total_mb * 1024) / usuarios
            print(f"\nEFICIENCIA DE ALMACENAMIENTO:")
            print(f"   KB por usuario: {kb_por_usuario:.1f} KB")
            print(f"   Entidades por MB: {(usuarios + productos + pedidos + detalles) / tamaño_total_mb:.1f}")
            print(f"   Espacio en índices: {total_indexes_mb:.2f} MB ({total_indexes_mb/tamaño_total_mb*100:.1f}%)")
        
        # Guardar en CSV
        with open('espacio_postgresql.csv', 'w') as f:
            f.write("metric,value\n")
            f.write(f"tamano_total_mb,{tamaño_total_mb:.2f}\n")
            f.write(f"usuarios,{usuarios}\n")
            f.write(f"productos,{productos}\n")
            f.write(f"pedidos,{pedidos}\n")
            f.write(f"detalles,{detalles}\n")
            f.write(f"kb_por_usuario,{kb_por_usuario:.1f}\n")
            f.write(f"total_indexes_mb,{total_indexes_mb:.2f}\n")
        
        print(f"\nInforme guardado en: espacio_postgresql.csv")
        
        cursor.close()
        conexion.close()
        
        return tamaño_total_mb
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # Configuración
    CONFIG = {
        "host": "localhost",
        "database": "benchmark_db",
        "user": "benchmark_user",
        "password": "benchmark123"
    }
    
    medir_espacio_bd(**CONFIG)