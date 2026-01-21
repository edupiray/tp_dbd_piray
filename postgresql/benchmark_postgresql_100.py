import psycopg2
import time
import csv
import statistics
import psutil
import sys
import random

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
    
    def run_single_query(self, query_name, query, params=None, iterations=10):
        print(f"\n{query_name} ({iterations} iteraciones)...")
        
        tiempos = []
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
                
                inicio = time.perf_counter()
                if current_params:
                    self.cursor.execute(query, current_params)
                else:
                    self.cursor.execute(query)
                
                if query.strip().upper().startswith('SELECT'):
                    self.cursor.fetchall()
                
                fin = time.perf_counter()
                tiempos.append((fin - inicio) * 1000)
                
            except Exception as e:
                print(f"  Error iteración {i+1}: {str(e)[:80]}...")
                continue
        
        if tiempos:
            stats = {
                'query': query_name,
                'iterations': len(tiempos),
                'min': min(tiempos),
                'max': max(tiempos),
                'avg': statistics.mean(tiempos),
                'median': statistics.median(tiempos),
                'std_dev': statistics.stdev(tiempos) if len(tiempos) > 1 else 0
            }
            print(f"  Avg: {stats['avg']:.2f}ms | Min: {stats['min']:.2f}ms | Max: {stats['max']:.2f}ms")
            self.resultados.append(stats)
    
    def run_full_benchmark(self):
        print("\n" + "="*60)
        print("BENCHMARK POSTGRESQL - CORREGIDO")
        print("="*60)
        
        # CONSULTAS CON query_name CORREGIDO
        consultas = [
            {
                "query_name": "Q1_Inserción_Simple",  # ¡CORREGIDO!
                "query": """
                    INSERT INTO usuario (nombre, email, pais, ciudad, fecha_registro)
                    VALUES (%s, %s, %s, %s, %s) RETURNING id
                """,
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
                "query": """
                    SELECT u.nombre, COUNT(p.id) as total_pedidos
                    FROM usuario u LEFT JOIN pedido p ON u.id = p.usuario_id
                    GROUP BY u.id, u.nombre ORDER BY total_pedidos DESC LIMIT 10
                """,
                "params": None
            },
            {
                "query_name": "Q5_Agregación_Compleja",
                "query": """
                    SELECT u.nombre, u.pais, AVG(p.total) as promedio_gasto
                    FROM usuario u JOIN pedido p ON u.id = p.usuario_id
                    GROUP BY u.id, u.nombre, u.pais
                    HAVING AVG(p.total) > %s ORDER BY promedio_gasto DESC LIMIT 10
                """,
                "params": (50,)
            },
            {
                "query_name": "Q6_Actualización_Masiva",
                "query": "UPDATE pedido SET estado = 'antiguo' WHERE fecha < %s",
                "params": ("2023-06-01",)
            },
            {
                "query_name": "Q7_Consulta_Compleja",
                "query": """
                    SELECT pr.nombre, pr.categoria, SUM(dp.cantidad) as total_vendido
                    FROM producto pr 
                    JOIN detalle_pedido dp ON pr.id = dp.producto_id
                    JOIN pedido p ON dp.pedido_id = p.id
                    JOIN usuario u ON p.usuario_id = u.id
                    WHERE u.pais = %s
                    GROUP BY pr.id, pr.nombre, pr.categoria
                    ORDER BY total_vendido DESC LIMIT 5
                """,
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
        
        print("\nBENCHMARK COMPLETADO")
    
    def save_results(self, filename="resultados_finales.csv"):
        if not self.resultados:
            print("Sin resultados")
            return
        
        campos = ['query', 'iterations', 'min', 'max', 'avg', 'median', 'std_dev']
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=campos)
            writer.writeheader()
            for r in self.resultados:
                row = {k: round(v, 4) if isinstance(v, float) else v for k, v in r.items() if k in campos}
                writer.writerow(row)
        print(f"Guardado: {filename}")

if __name__ == "__main__":
    print("Benchmark PostgreSQL - Corrección rápida")
    
    benchmark = PostgresBenchmark()
    try:
        benchmark.run_full_benchmark()
        benchmark.save_results()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        benchmark.close()