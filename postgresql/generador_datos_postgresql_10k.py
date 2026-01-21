import psycopg2
from psycopg2 import sql
from faker import Faker
import random
import time
from datetime import datetime, timedelta
import sys
import uuid  # <-- NUEVA IMPORTACIÓN para emails únicos

class PostgresDataGenerator:
    def __init__(self, host="localhost", database="benchmark_db", 
                 user="benchmark_user", password="benchmark123", port="5432"):
        """Inicializa el generador de datos para PostgreSQL"""
        
        self.conexion_config = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port
        }
        
        try:
            self.conexion = psycopg2.connect(**self.conexion_config)
            self.cursor = self.conexion.cursor()
            self.faker = Faker('es_ES')
            print("Conectado a PostgreSQL")
        except Exception as e:
            print(f"Error de conexión: {e}")
            sys.exit(1)
    
    def close(self):
        """Cierra la conexión a la base de datos"""
        if self.cursor:
            self.cursor.close()
        if self.conexion:
            self.conexion.close()
        print("Conexión cerrada")
    
    def generar_datos(self, num_usuarios=10000, num_productos=500, max_pedidos_por_usuario=5):
        """Genera datos para la escala especificada"""
        
        start_time = time.time()
        print(f"\nComenzando generación de datos...")
        print(f"   Usuarios: {num_usuarios:,}")
        print(f"   Productos: {num_productos:,}")
        print(f"   Pedidos por usuario: 1-{max_pedidos_por_usuario}")
        
        try:
            # 1. Generar productos
            productos_generados = self._generar_productos(num_productos)
            
            # 2. Generar usuarios y sus pedidos
            usuarios_generados, pedidos_generados, detalles_generados = self._generar_usuarios_y_pedidos(
                num_usuarios, max_pedidos_por_usuario, productos_generados
            )
            
            # Confirmar todos los cambios
            self.conexion.commit()
            
            elapsed_time = time.time() - start_time
            
            # Mostrar estadísticas
            print("\n" + "="*60)
            print("ESTADÍSTICAS DE LA GENERACIÓN")
            print("="*60)
            print(f"Tiempo total: {elapsed_time:.2f} segundos")
            print(f"Usuarios generados: {usuarios_generados:,}")
            print(f"Productos generados: {productos_generados:,}")
            print(f"Pedidos generados: {pedidos_generados:,}")
            print(f"Detalles de pedido: {detalles_generados:,}")
            print(f"Tasa: {(usuarios_generados + productos_generados + pedidos_generados + detalles_generados)/elapsed_time:.1f} entidades/segundo")
            
            return elapsed_time
            
        except Exception as e:
            print(f"Error durante la generación: {e}")
            self.conexion.rollback()
            raise
    
    def _generar_productos(self, num_productos):
        """Genera productos en la base de datos"""
        
        print(f"\nGenerando {num_productos:,} productos...")
        
        categorias = ['Electrónicos', 'Ropa', 'Hogar', 'Deportes', 'Libros', 'Juguetes', 'Belleza']
        marcas = ['TechCorp', 'StyleBrand', 'HomePro', 'SportMax', 'ReadWell', 'ToyFun', 'Glamour']
        
        batch_size = 1000
        productos_insertados = 0
        
        for i in range(0, num_productos, batch_size):
            batch = []
            batch_end = min(i + batch_size, num_productos)
            
            for j in range(i, batch_end):
                producto = (
                    f"{self.faker.word().capitalize()} {self.faker.word()}",
                    random.choice(categorias),
                    round(random.uniform(5.99, 999.99), 2),
                    random.choice(marcas)
                )
                batch.append(producto)
            
            # Insertar batch
            self.cursor.executemany(
                "INSERT INTO producto (nombre, categoria, precio, marca) VALUES (%s, %s, %s, %s)",
                batch
            )
            
            productos_insertados += len(batch)
            
            if batch_end % 1000 == 0 or batch_end == num_productos:
                print(f"   Procesados {batch_end:,}/{num_productos:,} productos...")
        
        return productos_insertados
    
    def _generar_usuarios_y_pedidos(self, num_usuarios, max_pedidos_por_usuario, max_producto_id):
        """Genera usuarios y sus pedidos asociados - VERSIÓN CORREGIDA PARA 10k"""
        
        print(f"\nGenerando {num_usuarios:,} usuarios con pedidos...")
        
        paises = ['Argentina', 'México', 'Colombia', 'Chile', 'España', 'Perú']
        estados_pedido = ['completado', 'pendiente', 'cancelado']
        
        # CONJUNTO para asegurar emails únicos
        emails_generados = set()
        
        usuarios_insertados = 0
        pedidos_insertados = 0
        detalles_insertados = 0
        batch_size = 500
        
        for i in range(1, num_usuarios + 1, batch_size):
            batch_end = min(i + batch_size, num_usuarios + 1)
            
            # Insertar usuarios en batch - CON EMAILS ÚNICOS GARANTIZADOS
            usuarios_batch = []
            for user_id in range(i, batch_end):
                fecha_registro = self.faker.date_between(start_date='-2y', end_date='today')
                
                # =======================================================
                # CORRECCIÓN CLAVE: Generar email único garantizado
                # =======================================================
                intentos = 0
                email_unico = None
                
                while intentos < 10:  # Máximo 10 intentos por usuario
                    # Opción 1: Usar UUID para garantizar unicidad
                    if intentos == 0:
                        # Usar base del email de Faker pero con UUID
                        base_email = self.faker.email().split('@')[0]
                        dominio = self.faker.free_email_domain()
                        email_unico = f"{base_email}_{user_id}_{int(time.time())}@{dominio}"
                    else:
                        # Si por algún motivo falla, usar UUID completo
                        email_unico = f"user_{user_id}_{uuid.uuid4().hex[:8]}@benchmark.com"
                    
                    # Verificar que no esté en el conjunto de emails ya generados
                    if email_unico not in emails_generados:
                        emails_generados.add(email_unico)
                        break
                    
                    intentos += 1
                
                if email_unico is None:
                    # Último recurso: email secuencial garantizado único
                    email_unico = f"usuario_{user_id}_{int(time.time()*1000)}@benchmark.com"
                    emails_generados.add(email_unico)
                
                usuario = (
                    self.faker.name(),
                    email_unico,
                    random.choice(paises),
                    self.faker.city(),
                    fecha_registro
                )
                usuarios_batch.append(usuario)
            
            # Insertar batch con manejo de conflictos
            try:
                self.cursor.executemany(
                    "INSERT INTO usuario (nombre, email, pais, ciudad, fecha_registro) VALUES (%s, %s, %s, %s, %s)",
                    usuarios_batch
                )
                usuarios_insertados += len(usuarios_batch)
                
            except psycopg2.errors.UniqueViolation:
                # Si hay duplicado, insertar uno por uno con manejo de errores
                print(f"Detectado posible duplicado en batch, insertando individualmente...")
                usuarios_insertados_en_batch = 0
                
                for usuario in usuarios_batch:
                    try:
                        self.cursor.execute(
                            "INSERT INTO usuario (nombre, email, pais, ciudad, fecha_registro) VALUES (%s, %s, %s, %s, %s)",
                            usuario
                        )
                        usuarios_insertados_en_batch += 1
                    except psycopg2.errors.UniqueViolation:
                        # Si aún hay duplicado, generar nuevo email y reintentar
                        nombre, email, pais, ciudad, fecha = usuario
                        nuevo_email = f"{email.split('@')[0]}_{uuid.uuid4().hex[:6]}@{email.split('@')[1]}"
                        try:
                            self.cursor.execute(
                                "INSERT INTO usuario (nombre, email, pais, ciudad, fecha_registro) VALUES (%s, %s, %s, %s, %s)",
                                (nombre, nuevo_email, pais, ciudad, fecha)
                            )
                            usuarios_insertados_en_batch += 1
                            print(f"Email corregido: {email[:30]}... → {nuevo_email[:30]}...")
                        except:
                            # Si falla, omitir este usuario
                            print(f"No se pudo insertar usuario duplicado, omitiendo")
                            continue
                
                usuarios_insertados += usuarios_insertados_en_batch
            
            # Para cada usuario en este batch, generar pedidos
            for user_offset in range(len(usuarios_batch)):
                user_actual_id = i + user_offset
                
                # Número aleatorio de pedidos para este usuario
                num_pedidos_usuario = random.randint(1, max_pedidos_por_usuario)
                
                for _ in range(num_pedidos_usuario):
                    # Crear pedido
                    fecha_pedido = self.faker.date_time_between(
                        start_date=datetime.now() - timedelta(days=365),
                        end_date=datetime.now()
                    )
                    estado = random.choice(estados_pedido)
                    
                    self.cursor.execute(
                        "INSERT INTO pedido (fecha, estado, usuario_id) VALUES (%s, %s, %s) RETURNING id",
                        (fecha_pedido, estado, user_actual_id)
                    )
                    pedido_id = self.cursor.fetchone()[0]
                    pedidos_insertados += 1
                    
                    # Añadir productos al pedido (detalle_pedido)
                    num_productos_pedido = random.randint(1, 4)
                    total_pedido = 0
                    
                    # Crear lista de productos únicos para este pedido
                    max_productos_a_seleccionar = min(num_productos_pedido, max_producto_id)
                    productos_seleccionados = random.sample(
                        range(1, max_producto_id + 1),
                        max_productos_a_seleccionar
                    )
                    
                    for producto_id in productos_seleccionados:
                        cantidad = random.randint(1, 3)
                        
                        # Obtener precio del producto
                        self.cursor.execute(
                            "SELECT precio FROM producto WHERE id = %s",
                            (producto_id,)
                        )
                        precio_unitario = self.cursor.fetchone()[0]
                        
                        subtotal = cantidad * precio_unitario
                        total_pedido += subtotal
                        
                        # Insertar detalle
                        self.cursor.execute(
                            """INSERT INTO detalle_pedido 
                               (pedido_id, producto_id, cantidad, precio_unitario, subtotal) 
                               VALUES (%s, %s, %s, %s, %s)""",
                            (pedido_id, producto_id, cantidad, precio_unitario, subtotal)
                        )
                        detalles_insertados += 1
                    
                    # Actualizar total del pedido
                    self.cursor.execute(
                        "UPDATE pedido SET total = %s WHERE id = %s",
                        (round(total_pedido, 2), pedido_id)
                    )
            
            if batch_end % 1000 == 0 or batch_end == num_usuarios:
                print(f"   Procesados {batch_end:,}/{num_usuarios:,} usuarios...")
                print(f"     Pedidos: {pedidos_insertados:,} | Detalles: {detalles_insertados:,}")
                print(f"     Emails únicos generados: {len(emails_generados):,}")
        
        return usuarios_insertados, pedidos_insertados, detalles_insertados

# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    # CONFIGURACIÓN
    CONFIG = {
        "host": "localhost",
        "database": "benchmark_db",
        "user": "benchmark_user",
        "password": "benchmark123",
        "port": "5432"
    }
    
    # =======================================================
    # CONFIGURACIÓN DE ESCALA - 10,000 USUARIOS
    # =======================================================
    ESCALA = "mediana"
    
    if ESCALA == "pequena":
        NUM_USUARIOS = 100
        NUM_PRODUCTOS = 50
        ARCHIVO_SALIDA = "generacion_postgres_100.csv"
    elif ESCALA == "mediana":
        NUM_USUARIOS = 10000
        NUM_PRODUCTOS = 500
        ARCHIVO_SALIDA = "generacion_postgres_10k.csv"
    elif ESCALA == "grande":
        NUM_USUARIOS = 100000
        NUM_PRODUCTOS = 5000
        ARCHIVO_SALIDA = "generacion_postgres_100k.csv"
    
    print("="*60)
    print("GENERADOR DE DATOS - POSTGRESQL (VERSIÓN CORREGIDA)")
    print(f"Escala: {ESCALA} ({NUM_USUARIOS:,} usuarios)")
    print("="*60)
    
    # Crear generador
    generador = PostgresDataGenerator(**CONFIG)
    
    try:
        # Limpiar datos existentes primero
        print("\nLimpiando datos anteriores...")
        generador.cursor.execute("TRUNCATE TABLE detalle_pedido, pedido, producto, usuario RESTART IDENTITY CASCADE;")
        generador.conexion.commit()
        print("Base de datos limpia")
        
        # Ejecutar generación
        tiempo = generador.generar_datos(
            num_usuarios=NUM_USUARIOS,
            num_productos=NUM_PRODUCTOS,
            max_pedidos_por_usuario=5
        )
        
        # Guardar métricas en CSV
        with open(ARCHIVO_SALIDA, 'w') as f:
            f.write("escala,usuarios,productos,tiempo_segundos\n")
            f.write(f"{ESCALA},{NUM_USUARIOS},{NUM_PRODUCTOS},{tiempo:.2f}\n")
        
        print(f"\nMétricas guardadas en: {ARCHIVO_SALIDA}")
        
    except Exception as e:
        print(f"\nError durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
    finally:
        generador.close()