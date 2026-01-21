import pymongo
import sys

def measure_mongodb_space(connection_uri="mongodb://localhost:27017/", db_name="benchmark_db"):
    """Mide el espacio utilizado por la base de datos MongoDB - VERSIÓN CORREGIDA PARA NUEVO DISEÑO"""
    try:
        client = pymongo.MongoClient(connection_uri)
        db = client[db_name]
        
        print("Midiendo espacio en disco de MongoDB...")
        
        # 1. Estadísticas de la base de datos (dbStats)
        db_stats = db.command("dbStats", scale=1024*1024)  # scale=1048576 para MB (1024*1024)
        
        # 2. Estadísticas por colección
        collections = db.list_collection_names()
        print(f"Colecciones encontradas: {collections}")
        
        collection_stats = []
        
        for col_name in collections:
            col_stats = db.command("collStats", col_name, scale=1024*1024)  # MB
            collection_stats.append({
                "nombre": col_name,
                "tamaño_mb": col_stats.get("size", 0),
                "almacenamiento_mb": col_stats.get("storageSize", 0),
                "indices_mb": col_stats.get("totalIndexSize", 0),
                "documentos": col_stats.get("count", 0)
            })
        
        # 3. Contar documentos en cada colección (usando count_documents)
        user_count = 0
        product_count = 0
        order_count = 0
        
        if "usuarios" in collections:
            user_count = db.usuarios.count_documents({})
        
        if "productos" in collections:
            product_count = db.productos.count_documents({})
        
        if "pedidos" in collections:
            order_count = db.pedidos.count_documents({})
        
        # 4. Calcular total de ítems en pedidos
        total_items = 0
        if "pedidos" in collections and order_count > 0:
            pipeline = [
                {"$unwind": "$items"},
                {"$group": {"_id": None, "total_items": {"$sum": "$items.cantidad"}}}
            ]
            try:
                result = list(db.pedidos.aggregate(pipeline))
                if result:
                    total_items = result[0]["total_items"]
            except Exception as e:
                print(f"  Nota: No se pudo calcular total de ítems: {e}")
                total_items = 0
        
        print("\n" + "="*60)
        print("INFORME DE ESPACIO - MONGODB (NUEVO DISEÑO)")
        print("="*60)
        
        print(f"\nESTADÍSTICAS DE REGISTROS:")
        print(f"   Usuarios: {user_count:,}")
        print(f"   Productos: {product_count:,}")
        print(f"   Pedidos: {order_count:,}")
        print(f"   Ítems en pedidos: {total_items:,}")
        
        print(f"\nESTADÍSTICAS DE LA BASE DE DATOS:")
        print(f"   Tamaño total (MB): {db_stats.get('dataSize', 0):,.2f}")
        print(f"   Almacenamiento total (MB): {db_stats.get('storageSize', 0):,.2f}")
        print(f"   Tamaño índices (MB): {db_stats.get('indexSize', 0):,.2f}")
        print(f"   Tamaño total + índices (MB): {db_stats.get('totalSize', 0):,.2f}")
        print(f"   Documentos totales: {db_stats.get('objects', 0):,}")
        print(f"   Tamaño promedio documento (B): {db_stats.get('avgObjSize', 0):,.2f}")
        
        print(f"\nDISTRIBUCIÓN POR COLECCIÓN (MB):")
        print("-"*70)
        print(f"{'Colección':<15} {'Docs':<10} {'Tamaño (MB)':<15} {'Almacen. (MB)':<15} {'Índices (MB)':<15}")
        print("-"*70)
        
        total_tablas_mb = 0
        for stats in collection_stats:
            print(f"{stats['nombre']:<15} {stats['documentos']:<10,} {stats['tamaño_mb']:<15.2f} "
                  f"{stats['almacenamiento_mb']:<15.2f} {stats['indices_mb']:<15.2f}")
            total_tablas_mb += stats['tamaño_mb']
        
        print("-"*70)
        print(f"{'TOTAL':<15} {'':<10} {total_tablas_mb:<15.2f}")
        
        # Calcular eficiencia
        if user_count > 0:
            mb_per_user = db_stats.get('totalSize', 0) / user_count
            kb_per_user = mb_per_user * 1024
            
            # Calcular entidades por MB
            total_entities = user_count + product_count + order_count
            entities_per_mb = total_entities / db_stats.get('totalSize', 1) if db_stats.get('totalSize', 0) > 0 else 0
            
            print(f"\nEFICIENCIA DE ALMACENAMIENTO:")
            print(f"   MB por usuario: {mb_per_user:.3f} MB")
            print(f"   KB por usuario: {kb_per_user:.1f} KB")
            print(f"   Entidades por MB: {entities_per_mb:.1f}")
            print(f"   Espacio en índices: {db_stats.get('indexSize', 0):.2f} MB ({db_stats.get('indexSize', 0)/db_stats.get('totalSize', 1)*100:.1f}%)")
        
        # Guardar en CSV
        with open('espacio_mongodb.csv', 'w') as f:
            f.write("metric,value\n")
            f.write(f"tamano_total_mb,{db_stats.get('totalSize', 0):.2f}\n")
            f.write(f"usuarios,{user_count}\n")
            f.write(f"productos,{product_count}\n")
            f.write(f"pedidos,{order_count}\n")
            f.write(f"items_pedidos,{total_items}\n")
            f.write(f"kb_por_usuario,{kb_per_user:.1f}\n")
            f.write(f"tamano_indices_mb,{db_stats.get('indexSize', 0):.2f}\n")
            f.write(f"entidades_por_mb,{entities_per_mb:.1f}\n")
        
        print(f"\nInforme guardado en: espacio_mongodb.csv")
        
        client.close()
        
        return db_stats.get('totalSize', 0)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # Configuración por defecto (local)
    CONFIG = {
        "connection_uri": "mongodb://localhost:27017/",
        "db_name": "benchmark_db"
    }
    
    measure_mongodb_space(**CONFIG)