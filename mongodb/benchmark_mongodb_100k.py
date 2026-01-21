import pymongo
import time
import csv
import statistics
import psutil
import random
from datetime import datetime, timedelta
import sys

class MongoDBBenchmark:
    def __init__(self, connection_uri="mongodb://localhost:27017/", db_name="benchmark_db"):
        self.client = pymongo.MongoClient(connection_uri)
        self.db = self.client[db_name]
        self.results = []
        print("Benchmark conectado a MongoDB")
    
    def close(self):
        self.client.close()
        print("Conexión cerrada")
    
    def _get_system_resources(self):
        """Obtiene CPU y memoria del sistema"""
        try:
            cpu = psutil.cpu_percent(interval=0.1)
            mem = psutil.virtual_memory()
            return {
                'cpu': cpu,
                'mem_used_mb': mem.used / (1024 ** 2),
                'mem_available_mb': mem.available / (1024 ** 2)
            }
        except:
            return {'cpu': 0, 'mem_used_mb': 0, 'mem_available_mb': 0}
    
    def run_single_query(self, query_name, query_func, iterations=10):
        print(f"\n{query_name} ({iterations} iteraciones)...")
        
        tiempos = []
        cpus = []
        mems = []
        
        for i in range(iterations):
            try:
                # Medir recursos ANTES
                recursos_antes = self._get_system_resources()
                
                # Ejecutar consulta
                inicio = time.perf_counter()
                result = query_func()
                fin = time.perf_counter()
                
                # Medir recursos DESPUÉS
                time.sleep(0.05)
                recursos_despues = self._get_system_resources()
                
                # Calcular métricas
                tiempo_ms = (fin - inicio) * 1000
                tiempos.append(tiempo_ms)
                
                # Delta de recursos
                cpu_delta = max(0, recursos_despues['cpu'] - recursos_antes['cpu'])
                mem_delta = recursos_despues['mem_used_mb'] - recursos_antes['mem_used_mb']
                
                cpus.append(cpu_delta)
                mems.append(mem_delta)
                
                # Limpiar resultado para no saturar memoria
                if result:
                    del result
                
            except Exception as e:
                print(f"  Error iteración {i+1}: {str(e)[:80]}...")
                continue
        
        if tiempos:
            stats = {
                'query': query_name,
                'iterations': len(tiempos),
                'min_ms': min(tiempos),
                'max_ms': max(tiempos),
                'avg_ms': statistics.mean(tiempos),
                'median_ms': statistics.median(tiempos),
                'std_dev_ms': statistics.stdev(tiempos) if len(tiempos) > 1 else 0,
                'cpu_avg': statistics.mean(cpus) if cpus else 0,
                'cpu_max': max(cpus) if cpus else 0,
                'mem_avg_mb': statistics.mean(mems) if mems else 0,
                'mem_max_mb': max(mems) if mems else 0
            }
            
            print(f"  Tiempo: {stats['avg_ms']:.2f}ms")
            print(f"  CPU: {stats['cpu_avg']:.1f}% | Memoria: {stats['mem_avg_mb']:.1f} MB")
            
            self.results.append(stats)
        else:
            print(f"  Todas las iteraciones fallaron")
    
    def run_full_benchmark(self):
        print("\n" + "="*60)
        print("BENCHMARK MONGODB - CON RECURSOS")
        print("="*60)
        
        # Mostrar estado inicial del sistema
        recursos_iniciales = self._get_system_resources()
        print(f"\nEstado inicial del sistema:")
        print(f"  CPU: {recursos_iniciales['cpu']:.1f}%")
        print(f"  Memoria usada: {recursos_iniciales['mem_used_mb']:.1f} MB")
        
        # CONSULTAS ADAPTADAS AL NUEVO DISEÑO (colecciones separadas)
        consultas = [
            {
                "query_name": "Q1_Inserción_Simple",
                "query_func": lambda: self.db.usuarios.insert_one({
                    "usuario_id": int(time.time() * 1000),
                    "nombre": f"Usuario Test {int(time.time())}",
                    "email": f"test_{int(time.time())}@benchmark.com",
                    "pais": "Test",
                    "ciudad": "Ciudad Benchmark",
                    "fecha_registro": datetime.now()
                })
            },
            {
                "query_name": "Q2_Búsqueda_Por_ID",
                "query_func": lambda: list(self.db.usuarios.find(
                    {"usuario_id": 1},
                    {"nombre": 1, "email": 1, "pais": 1}
                ).limit(1))
            },
            {
                "query_name": "Q3_Filtro_y_Agregación",
                "query_func": lambda: self.db.usuarios.count_documents({"pais": "Argentina"})
            },
            {
                "query_name": "Q4_JOIN_Simple",
                "query_func": lambda: list(self.db.pedidos.aggregate([
                    {"$group": {
                        "_id": "$usuario_id_ref",
                        "total_pedidos": {"$sum": 1}
                    }},
                    {"$sort": {"total_pedidos": -1}},
                    {"$limit": 10}
                ]))
            },
            {
                "query_name": "Q5_Agregación_Compleja",
                "query_func": lambda: list(self.db.pedidos.aggregate([
                    {"$group": {
                        "_id": "$usuario_id_ref",
                        "promedio_gasto": {"$avg": "$total"}
                    }},
                    {"$match": {"promedio_gasto": {"$gt": 50}}},
                    {"$sort": {"promedio_gasto": -1}},
                    {"$limit": 10}
                ]))
            },
            {
                "query_name": "Q6_Actualización_Masiva",
                "query_func": lambda: self.db.pedidos.update_many(
                    {"fecha": {"$lt": datetime(2023, 6, 1)}},
                    {"$set": {"estado": "antiguo"}}
                )
            },
            {
                "query_name": "Q7_Consulta_Compleja",
                "query_func": lambda: list(self.db.pedidos.aggregate([
                    {"$match": {"usuario.pais": "Argentina"}},  # Usando datos anidados del usuario
                    {"$unwind": "$items"},
                    {"$group": {
                        "_id": "$items.producto_id",
                        "nombre": {"$first": "$items.nombre"},
                        "categoria": {"$first": "$items.categoria"},
                        "total_vendido": {"$sum": "$items.cantidad"}
                    }},
                    {"$sort": {"total_vendido": -1}},
                    {"$limit": 5}
                ]))
            },
            {
                "query_name": "Q8_Eliminación",
                "query_func": lambda: self.db.usuarios.delete_many({
                    "email": {"$regex": "test_.*@benchmark.com"}
                })
            }
        ]
        
        for i, consulta in enumerate(consultas, 1):
            print(f"\n[{i}/8] {consulta['query_name']}")
            print("-"*40)
            self.run_single_query(**consulta)
            time.sleep(0.5)
        
        print("\nBENCHMARK COMPLETADO CON MÉTRICAS DE RECURSOS")
    
    def save_results(self, filename="resultados_mongodb_completos_100k.csv"):
        if not self.results:
            print("Sin resultados")
            return
        
        campos = [
            'query', 'iterations',
            'min_ms', 'max_ms', 'avg_ms', 'median_ms', 'std_dev_ms',
            'cpu_avg', 'cpu_max',
            'mem_avg_mb', 'mem_max_mb'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            for r in self.results:
                row = {}
                for campo in campos:
                    valor = r.get(campo, '')
                    if isinstance(valor, float):
                        row[campo] = round(valor, 4)
                    else:
                        row[campo] = valor
                writer.writerow(row)
        
        print(f"Resultados completos guardados en: {filename}")

if __name__ == "__main__":
    print("Benchmark MongoDB - Con métricas de CPU y Memoria")
    
    # Configuración
    CONFIG = {
        "connection_uri": "mongodb://localhost:27017/",
        "db_name": "benchmark_db"
    }
    
    benchmark = MongoDBBenchmark(**CONFIG)
    try:
        benchmark.run_full_benchmark()
        benchmark.save_results()
    except KeyboardInterrupt:
        print("\nBenchmark interrumpido")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        benchmark.close()