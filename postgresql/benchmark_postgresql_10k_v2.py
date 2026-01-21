import psycopg2
import time
import csv
import statistics
import psutil
import sys
import random
from datetime import datetime

class PostgresBenchmark:
    def __init__(self, host="localhost", database="benchmark_db", 
                 user="benchmark_user", password="benchmark123", port="5432"):
        
        self.conexion = psycopg2.connect(
            host=host, database=database, 
            user=user, password=password, port=port
        )
        self.conexion.autocommit = True
        self.cursor = self.conexion.cursor()
        self.resultados = []
        print("Benchmark conectado a PostgreSQL")
    
    def close(self):
        self.cursor.close()
        self.conexion.close()
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
    
    def run_single_query(self, query_name, query, params=None, iterations=10):
        print(f"\n{query_name} ({iterations} iteraciones)...")
        
        tiempos = []
        cpus = []
        mems = []
        
        for i in range(iterations):
            try:
                # Parámetros únicos para Q1
                current_params = params
                if query_name == "Q1_Inserción_Simple":
                    unique_id = f"{int(time.time()*1000)}_{i}_{random.randint(1000,9999)}"
                    current_params = (
                        f"Usuario {unique_id}",
                        f"benchmark_{unique_id}@test.com",
                        "Test", "Ciudad Benchmark", "2024-01-01"
                    )
                
                # Medir recursos ANTES
                recursos_antes = self._get_system_resources()
                
                # Ejecutar consulta
                inicio = time.perf_counter()
                
                if current_params:
                    self.cursor.execute(query, current_params)
                else:
                    self.cursor.execute(query)
                
                if query.strip().upper().startswith('SELECT'):
                    self.cursor.fetchall()
                
                fin = time.perf_counter()
                
                # Medir recursos DESPUÉS
                time.sleep(0.05)  # Pequeña pausa para estabilizar
                recursos_despues = self._get_system_resources()
                
                # Calcular métricas
                tiempo_ms = (fin - inicio) * 1000
                tiempos.append(tiempo_ms)
                
                # Delta de recursos durante la ejecución
                cpu_delta = max(0, recursos_despues['cpu'] - recursos_antes['cpu'])
                mem_delta = recursos_despues['mem_used_mb'] - recursos_antes['mem_used_mb']
                
                cpus.append(cpu_delta)
                mems.append(mem_delta)
                
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
            
            self.resultados.append(stats)
        else:
            print(f"  Todas las iteraciones fallaron")
    
    def run_full_benchmark(self):
        print("\n" + "="*60)
        print("BENCHMARK POSTGRESQL - CON RECURSOS")
        print("="*60)
        
        # Mostrar estado inicial del sistema
        recursos_iniciales = self._get_system_resources()
        print(f"\nEstado inicial del sistema:")
        print(f"  CPU: {recursos_iniciales['cpu']:.1f}%")
        print(f"  Memoria usada: {recursos_iniciales['mem_used_mb']:.1f} MB")
        
        # CONSULTAS (mismas que antes)
        consultas = [
            {
                "query_name": "Q1_Inserción_Simple",
                "query": "INSERT INTO usuario (nombre, email, pais, ciudad, fecha_registro) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                "params": None
            },
            {
                "query_name": "Q2_Búsqueda_Por_ID",
                "query": "SELECT nombre, email, pais FROM usuario WHERE id = %s",
                "params": (1,)
            },
            {
                "query_name": "Q3_Filtro_y_Agregación",
                "query": "SELECT COUNT(*) FROM usuario WHERE pais = %s",
                "params": ("Argentina",)
            },
            {
                "query_name": "Q4_JOIN_Simple",
                "query": "SELECT u.nombre, COUNT(p.id) as total_pedidos FROM usuario u LEFT JOIN pedido p ON u.id = p.usuario_id GROUP BY u.id, u.nombre ORDER BY total_pedidos DESC LIMIT 10",
                "params": None
            },
            {
                "query_name": "Q5_Agregación_Compleja",
                "query": "SELECT u.nombre, u.pais, AVG(p.total) as promedio_gasto FROM usuario u JOIN pedido p ON u.id = p.usuario_id GROUP BY u.id, u.nombre, u.pais HAVING AVG(p.total) > %s ORDER BY promedio_gasto DESC LIMIT 10",
                "params": (50,)
            },
            {
                "query_name": "Q6_Actualización_Masiva",
                "query": "UPDATE pedido SET estado = 'antiguo' WHERE fecha < %s",
                "params": ("2023-06-01",)
            },
            {
                "query_name": "Q7_Consulta_Compleja",
                "query": "SELECT pr.nombre, pr.categoria, SUM(dp.cantidad) as total_vendido FROM producto pr JOIN detalle_pedido dp ON pr.id = dp.producto_id JOIN pedido p ON dp.pedido_id = p.id JOIN usuario u ON p.usuario_id = u.id WHERE u.pais = %s GROUP BY pr.id, pr.nombre, pr.categoria ORDER BY total_vendido DESC LIMIT 5",
                "params": ("Argentina",)
            },
            {
                "query_name": "Q8_Eliminación",
                "query": "DELETE FROM usuario WHERE email LIKE %s",
                "params": ("benchmark_%@test.com",)
            }
        ]
        
        for i, consulta in enumerate(consultas, 1):
            print(f"\n[{i}/8] {consulta['query_name']}")
            print("-"*40)
            self.run_single_query(**consulta)
            time.sleep(0.5)
        
        print("\nBENCHMARK COMPLETADO CON MÉTRICAS DE RECURSOS")
    
    def save_results(self, filename="resultados_postgresql_completos_10k_v2.csv"):
        if not self.resultados:
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
            for r in self.resultados:
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
    print("Benchmark PostgreSQL - Con métricas de CPU y Memoria")
    
    benchmark = PostgresBenchmark()
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