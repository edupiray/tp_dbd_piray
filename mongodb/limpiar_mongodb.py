import pymongo

def clear_mongodb_database(connection_uri="mongodb://localhost:27017/", db_name="benchmark_db"):
    """Elimina todas las colecciones de la base de datos"""
    try:
        client = pymongo.MongoClient(connection_uri)
        db = client[db_name]
        
        collections = db.list_collection_names()
        if not collections:
            print("La base de datos ya está vacía")
            return
        
        print(f"Eliminando {len(collections)} colecciones de {db_name}...")
        for col in collections:
            db[col].drop()
            print(f"  ✓ Colección '{col}' eliminada")
        
        print("\nBase de datos vaciada exitosamente")
        client.close()
        
    except Exception as e:
        print(f"Error al vaciar la base de datos: {e}")

if __name__ == "__main__":
    clear_mongodb_database()