from neo4j import GraphDatabase
import time
import csv
import statistics

class Neo4jBenchmark:
    def __init__(self, uri, user, password):
        """Inicializa la conexión a Neo4j"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.results = []
        print("Benchmark conectado a Neo4j")
    
    def close(self):
        """Cierra la conexión"""
        self.driver.close()
        print("Conexión cerrada")
    
    def _execute_query(self, query, parameters=None):
        """Método interno para ejecutar una consulta y forzar la lectura completa"""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            # Forzar la materialización de todos los resultados para medir tiempo real
            return list(result)
    
    def _cleanup_benchmark_data(self):
        """LIMPIEZA CRÍTICA: Elimina TODOS los datos de pruebas anteriores del benchmark"""
        print("Realizando limpieza profunda de datos de benchmark previos...")
        
        cleanup_queries = [
            # 1. Eliminar usuarios de prueba por ID, nombre o email
            "MATCH (u:Usuario) WHERE u.id = 999999 OR u.nombre CONTAINS 'Benchmark' OR u.email CONTAINS 'benchmark' DETACH DELETE u",
            # 2. Eliminar pedidos con etiqueta 'antiguo' (de Q6 previas)
            "MATCH (p:Pedido) WHERE p.etiqueta = 'antiguo' DETACH DELETE p",
            # 3. Eliminar relaciones de amistad creadas por el benchmark
            "MATCH ()-[r:AMIGO_DE]->() WHERE r.desde = date('2023-01-01') DELETE r"
        ]
        
        for i, query in enumerate(cleanup_queries, 1):
            try:
                result = self._execute_query(query)
                print(f"Limpieza {i}/3 completada")
            except Exception as e:
                print(f"Advertencia en limpieza {i}: {str(e)[:80]}...")
        
        # Verificación final
        verify_query = "MATCH (u:Usuario {id: 999999}) RETURN count(u) as remaining"
        try:
            count = self._execute_query(verify_query)[0]["remaining"]
            if count == 0:
                print("Verificación: No quedan usuarios conflictivos")
            else:
                print(f"Aún existen {count} usuarios conflictivos")
        except:
            pass
    
    def _prepare_test_data(self):
        """Prepara datos específicos para las pruebas"""
        print("Preparando datos de prueba...")
        
        # 1. Crear índices esenciales (si no existen)
        index_queries = [
            "CREATE INDEX IF NOT EXISTS FOR (u:Usuario) ON (u.id)",
            "CREATE INDEX IF NOT EXISTS FOR (u:Usuario) ON (u.pais)",
            "CREATE INDEX IF NOT EXISTS FOR (p:Pedido) ON (p.fecha)",
            "CREATE INDEX IF NOT EXISTS FOR (p:Producto) ON (p.nombre)"
        ]
        
        for query in index_queries:
            try:
                self._execute_query(query)
            except Exception as e:
                print(f"Nota sobre índice: {str(e)[:80]}...")
        
        # 2. Crear relaciones de AMIGO_DE para Q7 (solo si no existen suficientes)
        try:
            amigo_count = self._execute_query(
                "MATCH ()-[r:AMIGO_DE]->() RETURN count(r) as total"
            )[0]["total"]
            
            if amigo_count < 10:  # Si hay pocas relaciones, crear algunas
                self._execute_query("""
                    MATCH (u1:Usuario), (u2:Usuario)
                    WHERE u1.id < 20 AND u2.id < 20 AND u1.id <> u2.id AND rand() < 0.2
                    MERGE (u1)-[:AMIGO_DE {desde: date('2023-01-01'), tipo: 'benchmark'}]->(u2)
                    RETURN count(*) as creadas
                """)
                print("Relaciones AMIGO_DE preparadas")
            else:
                print(f"Ya existen {amigo_count} relaciones AMIGO_DE")
        except Exception as e:
            print(f"No se pudieron crear relaciones AMIGO_DE: {str(e)[:80]}...")
        
        # 3. Asegurar que existe usuario con id=1 para las consultas
        try:
            user1_exists = self._execute_query(
                "MATCH (u:Usuario {id: 1}) RETURN count(u) > 0 as existe"
            )[0]["existe"]
            
            if not user1_exists:
                print("Creando usuario con id=1 para pruebas...")
                self._execute_query("""
                    MERGE (u:Usuario {id: 1})
                    ON CREATE SET u.nombre = 'Usuario Test 1',
                                  u.email = 'test1@benchmark.com',
                                  u.pais = 'Argentina',
                                  u.ciudad = 'Buenos Aires',
                                  u.fecha_registro = date('2024-01-01')
                """)
        except:
            pass
        
        print("Preparación de datos completada")
    
    def _clear_caches(self):
        """Limpia las cachés de consultas para mediciones más consistentes"""
        try:
            self._execute_query("CALL db.clearQueryCaches()")
        except Exception:
            pass  # Si falla (permisos), continuamos igual
    
    def run_single_query(self, query_name, query_template, params=None, iterations=10):
        """Ejecuta una consulta múltiples veces y captura métricas detalladas"""
        print(f"Ejecutando {query_name} ({iterations} iteraciones)...")
        
        times = []
        params = params or {}
        
        for i in range(iterations):
            start_time = time.perf_counter()
            
            try:
                self._execute_query(query_template, params)
            except Exception as e:
                print(f"Error en iteración {i+1}: {str(e)[:100]}...")
                raise
            
            end_time = time.perf_counter()
            elapsed_ms = (end_time - start_time) * 1000
            times.append(elapsed_ms)
        
        # Calcular estadísticas
        stats = {
            'query': query_name,
            'iterations': iterations,
            'min': min(times),
            'max': max(times),
            'avg': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }
        
        self.results.append(stats)
        print(f"Avg: {stats['avg']:.2f}ms | Min: {stats['min']:.2f}ms | Max: {stats['max']:.2f}ms")
        
        return stats
    
    def run_full_benchmark(self):
        """Ejecuta el benchmark completo con las 8 consultas definidas"""
        print("\nINICIANDO BENCHMARK COMPLETO")
        print("=" * 60)
        
        # PASO CRÍTICO 1: Limpieza exhaustiva
        self._cleanup_benchmark_data()
        
        # PASO CRÍTICO 2: Preparar datos de prueba
        self._prepare_test_data()
        
        # DEFINICIÓN DE CONSULTAS DE PRUEBA (VERSION IDEMPOTENTE)
        queries = [
            {
                "name": "Q1_Inserción_Simple",
                "query": """
                    MERGE (u:Usuario {id: $id_usuario})
                    ON CREATE SET u.nombre = $nombre,
                                  u.email = $email,
                                  u.pais = $pais,
                                  u.ciudad = 'Ciudad Benchmark',
                                  u.fecha_registro = date('2024-01-01')
                    RETURN u.id
                """,
                "params": {
                    "id_usuario": 999999,
                    "nombre": "Usuario Benchmark",
                    "email": "benchmark@test.com",
                    "pais": "Test"
                }
            },
            {
                "name": "Q2_Búsqueda_Por_ID",
                "query": "MATCH (u:Usuario {id: $id}) RETURN u.nombre, u.email, u.pais",
                "params": {"id": 1}
            },
            {
                "name": "Q3_Filtro_y_Agregación",
                "query": "MATCH (u:Usuario) WHERE u.pais = $pais RETURN count(u) as total_usuarios",
                "params": {"pais": "Argentina"}
            },
            {
                "name": "Q4_JOIN_Simple",
                "query": """
                    MATCH (u:Usuario)-[:REALIZÓ]->(p:Pedido)
                    RETURN u.nombre, count(p) as total_pedidos
                    ORDER BY total_pedidos DESC
                    LIMIT 10
                """,
                "params": {}
            },
            {
                "name": "Q5_Agregación_Compleja",
                "query": """
                    MATCH (u:Usuario)-[:REALIZÓ]->(p:Pedido)
                    WITH u, avg(p.total) as promedio_gasto
                    WHERE promedio_gasto > $monto_minimo
                    RETURN u.nombre, u.pais, promedio_gasto
                    ORDER BY promedio_gasto DESC
                    LIMIT 10
                """,
                "params": {"monto_minimo": 50}
            },
            {
                "name": "Q6_Actualización_Masiva",
                "query": """
                    MATCH (p:Pedido)
                    WHERE p.fecha < datetime($fecha_limite)
                    SET p.etiqueta = 'antiguo'
                    RETURN count(p) as pedidos_actualizados
                """,
                "params": {"fecha_limite": "2023-12-01T00:00:00"}
            },
            {
                "name": "Q7_Camino_Grafo_Amigos",
                "query": """
                    MATCH (u:Usuario {id: $id_usuario})-[:AMIGO_DE*1..2]->(amigo:Usuario)
                    MATCH (amigo)-[:REALIZÓ]->(pedido:Pedido)-[:CONTIENE]->(producto:Producto)
                    WHERE NOT (u)-[:REALIZÓ]->(:Pedido)-[:CONTIENE]->(producto)
                    RETURN producto.nombre, count(DISTINCT amigo) as recomendado_por
                    ORDER BY recomendado_por DESC
                    LIMIT 5
                """,
                "params": {"id_usuario": 1}
            },
            {
                "name": "Q8_Eliminación",
                "query": "MATCH (u:Usuario {id: $id}) DETACH DELETE u RETURN 'Eliminado' as resultado",
                "params": {"id": 999999}
            }
        ]
        
        # Ejecutar cada consulta del benchmark
        for i, q in enumerate(queries):
            print(f"\n[{i+1}/8] {q['name']}")
            print("-" * 40)
            
            # Limpiar caché antes de cada operación de escritura
            if q["name"] in ["Q1_Inserción_Simple", "Q6_Actualización_Masiva", "Q8_Eliminación"]:
                self._clear_caches()
            
            # Ejecutar la consulta con manejo de errores
            try:
                self.run_single_query(
                    query_name=q["name"],
                    query_template=q["query"],
                    params=q["params"],
                    iterations=10
                )
            except Exception as e:
                print(f"ERROR en {q['name']}: {str(e)[:100]}...")
                print(f"Saltando a la siguiente consulta...")
                # Registrar resultado fallido
                self.results.append({
                    'query': q["name"],
                    'iterations': 0,
                    'min': 0,
                    'max': 0,
                    'avg': 0,
                    'median': 0,
                    'std_dev': 0,
                    'error': str(e)[:200]
                })
            
            # Pequeña pausa entre consultas diferentes
            if i < len(queries) - 1:
                time.sleep(0.5)
        
        print("\n" + "=" * 60)
        print("BENCHMARK COMPLETADO")
        print("=" * 60)
    
    def save_results(self, filename="resultados_benchmark.csv"):
        """Guarda los resultados en un archivo CSV para análisis posterior"""
        if not self.results:
            print("No hay resultados para guardar")
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['query', 'iterations', 'min', 'max', 'avg', 'median', 'std_dev']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in self.results:
                # Filtrar solo los campos que queremos en el CSV
                row = {k: v for k, v in result.items() if k in fieldnames}
                writer.writerow(row)
        
        print(f"Resultados guardados en: {filename}")
        print(f"Total de consultas evaluadas: {len([r for r in self.results if r.get('iterations', 0) > 0])}")
    
    def print_summary(self):
        """Imprime un resumen de los resultados en consola"""
        valid_results = [r for r in self.results if r.get('iterations', 0) > 0]
        
        if not valid_results:
            print("No hay resultados válidos para mostrar")
            return
        
        print("\n" + "=" * 60)
        print("RESUMEN DE RESULTADOS")
        print("=" * 60)
        print(f"{'Consulta':<25} {'Avg (ms)':<12} {'Min (ms)':<12} {'Max (ms)':<12} {'Iter':<6}")
        print("-" * 60)
        
        for result in valid_results:
            print(f"{result['query']:<25} {result['avg']:<12.2f} {result['min']:<12.2f} {result['max']:<12.2f} {result.get('iterations', 0):<6}")

# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    # CONFIGURACIÓN (AJUSTA SEGÚN TU ENTORNO)
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "admin123"  # <-- ¡CAMBIAR POR TU CONTRASEÑA REAL!
    
    print("=" * 60)
    print("BENCHMARK DE RENDIMIENTO - NEO4J")
    print("Proyecto de Maestría en Ingeniería de Software")
    print("=" * 60)
    
    # Crear instancia del benchmark
    benchmark = Neo4jBenchmark(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        # Ejecutar benchmark completo
        benchmark.run_full_benchmark()
        
        # Guardar resultados
        benchmark.save_results()
        
        # Mostrar resumen
        benchmark.print_summary()
        
    except KeyboardInterrupt:
        print("\n\nBenchmark interrumpido por el usuario")
    except Exception as e:
        print(f"\nERROR durante la ejecución: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cerrar conexión
        benchmark.close()