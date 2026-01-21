import pymongo
from faker import Faker
import random
import time
from datetime import datetime, timedelta
import sys
import uuid
import argparse
from bson import Decimal128

class MongoDBDataGenerator:
    def __init__(self, connection_uri="mongodb://localhost:27017/", db_name="benchmark_db"):
        """Inicializa el generador de datos para MongoDB - VERSIÓN CORREGIDA"""
        try:
            self.client = pymongo.MongoClient(connection_uri)
            self.db = self.client[db_name]
            self.faker = Faker('es_ES')
            print(f"Conectado a MongoDB: {connection_uri}")
            print(f"Base de datos: {db_name}")
        except Exception as e:
            print(f"Error de conexión: {e}")
            sys.exit(1)
    
    def clear_database(self):
        """Elimina todas las colecciones de la base de datos"""
        collections = self.db.list_collection_names()
        for col in collections:
            self.db[col].drop()
        print("Base de datos limpiada")
    
    def generate_data(self, num_users=10000, num_products=500, max_orders_per_user=5):
        """Genera datos para la escala especificada"""
        start_time = time.time()
        print(f"\nComenzando generación de datos...")
        print(f"   Usuarios: {num_users:,}")
        print(f"   Productos: {num_products:,}")
        print(f"   Pedidos por usuario: 1-{max_orders_per_user}")
        
        # 1. Generar productos (colección separada)
        products_generated = self._generate_products(num_products)
        
        # 2. Generar usuarios (colección separada)
        users_generated = self._generate_users(num_users)
        
        # 3. Generar pedidos con referencias
        orders_generated = self._generate_orders(num_users, max_orders_per_user, products_generated)
        
        elapsed_time = time.time() - start_time
        
        # Mostrar estadísticas
        print("\n" + "="*60)
        print("ESTADÍSTICAS DE LA GENERACIÓN - MONGODB")
        print("="*60)
        print(f"Tiempo total: {elapsed_time:.2f} segundos")
        print(f"Usuarios generados: {users_generated:,}")
        print(f"Productos generados: {products_generated:,}")
        print(f"Pedidos generados: {orders_generated:,}")
        total_entities = users_generated + products_generated + orders_generated
        print(f"Tasa: {total_entities/elapsed_time:.1f} entidades/segundo")
        
        return elapsed_time
    
    def _generate_products(self, num_products):
        """Genera productos en la colección productos"""
        print(f"\nGenerando {num_products:,} productos...")
        
        categories = ['Electrónicos', 'Ropa', 'Hogar', 'Deportes', 'Libros', 'Juguetes', 'Belleza']
        brands = ['TechCorp', 'StyleBrand', 'HomePro', 'SportMax', 'ReadWell', 'ToyFun', 'Glamour']
        
        products = []
        for i in range(1, num_products + 1):
            price = round(random.uniform(5.99, 999.99), 2)
            product = {
                "producto_id": i,
                "nombre": f"{self.faker.word().capitalize()} {self.faker.word()}",
                "categoria": random.choice(categories),
                "precio": price,
                "marca": random.choice(brands),
                "fecha_creacion": self.faker.date_time_this_year()
            }
            products.append(product)
            
            # Mostrar progreso cada 1000 productos
            if i % 1000 == 0 or i == num_products:
                print(f"   Procesados {i:,}/{num_products:,} productos...")
        
        # Insertar en lote
        if products:
            self.db.productos.insert_many(products)
        
        return len(products)
    
    def _generate_users(self, num_users):
        """Genera usuarios en colección separada"""
        print(f"\nGenerando {num_users:,} usuarios...")
        
        countries = ['Argentina', 'México', 'Colombia', 'Chile', 'España', 'Perú']
        
        users_batch = []
        users_generated = 0
        last_progress_shown = 0
        
        for user_id in range(1, num_users + 1):
            # Datos del usuario
            registration_date = self.faker.date_time_between(
                start_date='-2y', 
                end_date='now'
            )
            
            # Generar email único
            base_email = self.faker.email().split('@')[0]
            domain = self.faker.free_email_domain()
            unique_email = f"{base_email}_{user_id}_{int(time.time())}@{domain}"
            
            user_data = {
                "usuario_id": user_id,
                "nombre": self.faker.name(),
                "email": unique_email,
                "pais": random.choice(countries),
                "ciudad": self.faker.city(),
                "fecha_registro": registration_date
            }
            
            users_batch.append(user_data)
            users_generated += 1
            
            # Insertar en lotes de 1000
            if len(users_batch) >= 1000 or user_id == num_users:
                self.db.usuarios.insert_many(users_batch)
                users_batch = []
            
            # Mostrar progreso cada 1000 usuarios
            if users_generated - last_progress_shown >= 1000:
                print(f"   Procesados {users_generated:,}/{num_users:,} usuarios...")
                last_progress_shown = users_generated
        
        return users_generated
    
    def _generate_orders(self, num_users, max_orders_per_user, max_product_id):
        """Genera pedidos con datos anidados de usuario y productos"""
        print(f"\nGenerando pedidos para {num_users:,} usuarios...")
        
        order_statuses = ['completado', 'pendiente', 'cancelado']
        orders_generated = 0
        last_progress_shown = 0
        
        # Obtener todos los usuarios para anidar datos
        users_dict = {}
        for user in self.db.usuarios.find({}, {"_id": 0, "usuario_id": 1, "nombre": 1, "email": 1, "pais": 1, "ciudad": 1}):
            users_dict[user["usuario_id"]] = user
        
        # Obtener todos los productos para anidar datos
        products_dict = {}
        for product in self.db.productos.find({}, {"_id": 0, "producto_id": 1, "nombre": 1, "categoria": 1, "precio": 1}):
            products_dict[product["producto_id"]] = product
        
        # Generar pedidos por lote
        batch_size = 500
        orders_batch = []
        
        for user_id in range(1, num_users + 1):
            # Número aleatorio de pedidos para este usuario
            num_orders = random.randint(1, max_orders_per_user)
            
            # Obtener datos del usuario para anidar
            user_data = users_dict.get(user_id, {
                "usuario_id": user_id,
                "nombre": f"Usuario {user_id}",
                "email": f"user{user_id}@example.com",
                "pais": "Argentina",
                "ciudad": "Ciudad"
            })
            
            for order_num in range(num_orders):
                # Fecha del pedido
                order_date = self.faker.date_time_between(
                    start_date=datetime.now() - timedelta(days=365),
                    end_date=datetime.now()
                )
                
                # Seleccionar productos para este pedido
                num_items = random.randint(1, 4)
                available_products = list(products_dict.keys())
                selected_product_ids = random.sample(
                    available_products, 
                    min(num_items, len(available_products))
                )
                
                items = []
                order_total = 0
                
                for prod_id in selected_product_ids:
                    product_data = products_dict.get(prod_id, {
                        "producto_id": prod_id,
                        "nombre": f"Producto {prod_id}",
                        "categoria": "General",
                        "precio": 10.0
                    })
                    
                    quantity = random.randint(1, 3)
                    unit_price = float(product_data["precio"])
                    subtotal = quantity * unit_price
                    order_total += subtotal
                    
                    item = {
                        "producto_id": prod_id,
                        "nombre": product_data["nombre"],
                        "categoria": product_data["categoria"],
                        "precio_unitario": unit_price,
                        "cantidad": quantity,
                        "subtotal": round(subtotal, 2)
                    }
                    items.append(item)
                
                order = {
                    "pedido_id": orders_generated + 1,
                    "fecha": order_date,
                    "estado": random.choice(order_statuses),
                    "total": round(order_total, 2),
                    "usuario": user_data,  # Datos del usuario anidados (solo lectura)
                    "items": items,  # Items del pedido anidados
                    "usuario_id_ref": user_id  # Referencia adicional por si se necesita
                }
                
                orders_batch.append(order)
                orders_generated += 1
            
            # Insertar en lotes
            if len(orders_batch) >= batch_size:
                self.db.pedidos.insert_many(orders_batch)
                orders_batch = []
            
            # Mostrar progreso cada 1000 usuarios procesados
            if user_id - last_progress_shown >= 1000:
                print(f"   Procesados {user_id:,}/{num_users:,} usuarios...")
                print(f"     Pedidos generados: {orders_generated:,}")
                last_progress_shown = user_id
        
        # Insertar cualquier pedido restante
        if orders_batch:
            self.db.pedidos.insert_many(orders_batch)
        
        return orders_generated

# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generador de datos para MongoDB - VERSIÓN CORREGIDA')
    parser.add_argument('--scale', choices=['small', 'medium', 'large'], default='medium',
                       help='Escala de datos: small (100), medium (10k), large (100k)')
    parser.add_argument('--uri', default='mongodb://localhost:27017/',
                       help='URI de conexión a MongoDB')
    parser.add_argument('--db', default='benchmark_db',
                       help='Nombre de la base de datos')
    
    args = parser.parse_args()
    
    # Configurar escala
    if args.scale == 'small':
        num_users = 100
        num_products = 50
        output_file = "generacion_mongodb_100.csv"
    elif args.scale == 'medium':
        num_users = 10000
        num_products = 500
        output_file = "generacion_mongodb_10k.csv"
    elif args.scale == 'large':
        num_users = 100000
        num_products = 5000
        output_file = "generacion_mongodb_100k.csv"
    
    print("="*60)
    print("GENERADOR DE DATOS - MONGODB (VERSIÓN CORREGIDA SIN RECURSIVIDAD)")
    print(f"Escala: {args.scale} ({num_users:,} usuarios)")
    print("="*60)
    
    # Crear generador
    generator = MongoDBDataGenerator(connection_uri=args.uri, db_name=args.db)
    
    try:
        # Limpiar datos existentes
        print("\nLimpiando datos anteriores...")
        generator.clear_database()
        
        # Ejecutar generación
        tiempo = generator.generate_data(
            num_users=num_users,
            num_products=num_products,
            max_orders_per_user=5
        )
        
        # Guardar métricas
        with open(output_file, 'w') as f:
            f.write("escala,usuarios,productos,tiempo_segundos\n")
            f.write(f"{args.scale},{num_users},{num_products},{tiempo:.2f}\n")
        
        print(f"\nMétricas guardadas en: {output_file}")
        
        # Mostrar resumen final
        print("\n" + "="*60)
        print("RESUMEN DE COLECCIONES CREADAS:")
        print("="*60)
        collections = generator.db.list_collection_names()
        for col in collections:
            count = generator.db[col].count_documents({})
            print(f"  {col}: {count:,} documentos")
        
    except Exception as e:
        print(f"\nError durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
    finally:
        generator.client.close()