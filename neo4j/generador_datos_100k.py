from neo4j import GraphDatabase
from faker import Faker
import random
import time

class Neo4jDataGenerator:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.faker = Faker('es_ES')
        print("Conectado a Neo4j")
        
    def close(self):
        self.driver.close()
        print("🔌 Conexión cerrada")
    
    def generar_datos_escala_pequena(self, num_usuarios=100000, num_productos=5000, max_pedidos_por_usuario=5):
        start_time = time.time()
        print(f"Comenzando generación de datos...")
        print(f"Usuarios: {num_usuarios}")
        print(f" Productos: {num_productos}")
        
        # 1. Generar productos
        self._generar_productos(num_productos)
        
        # 2. Generar usuarios y sus pedidos
        with self.driver.session() as session:
            order_id = 1
            for user_id in range(1, num_usuarios + 1):
                # Crear usuario
                session.execute_write(self._crear_usuario, user_id)
                
                # Crear pedidos para este usuario
                num_pedidos = random.randint(1, max_pedidos_por_usuario)
                for _ in range(num_pedidos):
                    session.execute_write(self._crear_pedido_completo, order_id, user_id)
                    order_id += 1
                
                if user_id % 10 == 0:
                    print(f"   Procesados {user_id}/{num_usuarios} usuarios...")
        
        elapsed_time = time.time() - start_time
        print(f"¡Generación completada en {elapsed_time:.2f} segundos!")
        return elapsed_time
    
    def _generar_productos(self, count):
        categorias = ['Electrónicos', 'Ropa', 'Hogar', 'Deportes', 'Libros', 'Juguetes', 'Belleza']
        marcas = ['TechCorp', 'StyleBrand', 'HomePro', 'SportMax', 'ReadWell', 'ToyFun', 'Glamour']
        
        with self.driver.session() as session:
            for i in range(count):
                session.execute_write(
                    self._crear_producto,  # ¡Este método SÍ existe ahora!
                    product_id=1000 + i,
                    nombre=f"{self.faker.word().capitalize()} {self.faker.word()}",
                    categoria=random.choice(categorias),
                    precio=round(random.uniform(5.99, 999.99), 2),
                    marca=random.choice(marcas)
                )
        print(f"{count} productos creados")
    
    # --- MÉTODOS AUXILIARES (ESTÁTICOS) ---
    
    @staticmethod
    def _crear_usuario(tx, user_id):
        """Crea un nodo Usuario (método estático)"""
        fake = Faker('es_ES')
        query = """
        CREATE (u:Usuario {
            id: $user_id,
            nombre: $nombre,
            email: $email,
            pais: $pais,
            ciudad: $ciudad,
            fecha_registro: date($fecha_registro)
        })
        """
        fecha_registro = fake.date_between(start_date='-2y', end_date='today')
        tx.run(query, 
               user_id=user_id,
               nombre=fake.name(),
               email=fake.email(),
               pais=random.choice(['Argentina', 'México', 'Colombia', 'Chile', 'España', 'Perú']),
               ciudad=fake.city(),
               fecha_registro=str(fecha_registro))
    
    @staticmethod
    def _crear_producto(tx, product_id, nombre, categoria, precio, marca):
        """Crea un nodo Producto (método estático)"""
        query = """
        CREATE (p:Producto {
            id: $product_id,
            nombre: $nombre,
            categoria: $categoria,
            precio: $precio,
            marca: $marca
        })
        """
        tx.run(query,
               product_id=product_id,
               nombre=nombre,
               categoria=categoria,
               precio=precio,
               marca=marca)
    
    @staticmethod
    def _crear_pedido_completo(tx, order_id, user_id):
        """Crea un nodo Pedido y lo conecta con productos aleatorios"""
        fake = Faker('es_ES')
        
        # Crear el nodo Pedido
        query_pedido = """
        MATCH (u:Usuario {id: $user_id})
        CREATE (u)-[:REALIZÓ]->(p:Pedido {
            id: $order_id,
            fecha: datetime($fecha),
            total: $total,
            estado: $estado
        })
        RETURN p
        """
        fecha_pedido = fake.date_time_between(start_date='-1y', end_date='now')
        total = 0
        
        # Crear el pedido con total temporal 0
        tx.run(query_pedido, 
               order_id=order_id, 
               user_id=user_id, 
               fecha=fecha_pedido.isoformat(),
               total=0,
               estado=random.choice(['completado', 'pendiente', 'cancelado']))
        
        # Añadir productos (1-4 productos por pedido)
        num_productos = random.randint(1, 4)
        for _ in range(num_productos):
            product_id = random.randint(1000, 5999)  # IDs de productos generados (5000 productos)
            cantidad = random.randint(1, 3)
            precio_unitario = round(random.uniform(10, 500), 2)
            subtotal = cantidad * precio_unitario
            total += subtotal
            
            query_contenido = """
            MATCH (ped:Pedido {id: $order_id})
            MATCH (prod:Producto {id: $product_id})
            CREATE (ped)-[:CONTIENE {
                cantidad: $cantidad,
                precio_unitario: $precio_unitario,
                subtotal: $subtotal
            }]->(prod)
            """
            tx.run(query_contenido,
                   order_id=order_id,
                   product_id=product_id,
                   cantidad=cantidad,
                   precio_unitario=precio_unitario,
                   subtotal=subtotal)
        
        # Actualizar el total del pedido
        tx.run("""
            MATCH (p:Pedido {id: $order_id})
            SET p.total = $total
            """, order_id=order_id, total=round(total, 2))




# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    # CONFIGURACIÓN (¡MODIFICA ESTO SI ES NECESARIO!)
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "admin123"  # <-- Cambia por tu contraseña real
    
    # PARÁMETROS DE ESCALA MEDIA-ALTA
    NUM_USUARIOS = 100000      # <-- Ajustable
    NUM_PRODUCTOS = 5000      # <-- Ajustable
    
    print("=" * 50)
    print("GENERADOR DE DATOS PARA PROYECTO DE MAESTRÍA")
    print("=" * 50)
    
    # Crear generador y ejecutar
    generador = Neo4jDataGenerator(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        tiempo_ejecucion = generador.generar_datos_escala_pequena(
            num_usuarios=NUM_USUARIOS,
            num_productos=NUM_PRODUCTOS,
            max_pedidos_por_usuario=5
        )
        
        # Mostrar estadísticas
        print("\n" + "=" * 50)
        print("ESTADÍSTICAS DE LA GENERACIÓN")
        print("=" * 50)
        print(f"Tiempo total: {tiempo_ejecucion:.2f} segundos")
        print(f"Usuarios generados: {NUM_USUARIOS}")
        print(f"Productos generados: {NUM_PRODUCTOS}")
        print(f"Pedidos estimados: ~{NUM_USUARIOS * 3}")  # Promedio 3 por usuario
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        generador.close()