from neo4j import GraphDatabase
import time
import csv
import statistics
import psutil  # <-- NUEVA IMPORTACIÓN para monitorear recursos del sistema
import os

class ResourceMonitor:
    """
    Clase auxiliar para monitorear el uso de recursos (CPU, RAM) 
    del proceso de Neo4j durante la ejecución de consultas.
    """
    def __init__(self, process_name="neo4j"):
        """
        Inicializa el monitor de recursos.
        
        Args:
            process_name: Nombre del proceso a monitorear. 
                         En Windows podría ser 'neo4j.bat' o 'java'.
        """
        self.target_pid = None
        self.target_process = None
        self._find_neo4j_process(process_name)
    
    def _find_neo4j_process(self, process_name):
        """Busca y vincula el proceso de Neo4j para monitoreo."""
        print(f"Buscando proceso de Neo4j para monitoreo...")
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                # Verificar si es el proceso de Neo4j
                if (process_name.lower() in proc.info['name'].lower() or 
                    any('neo4j' in (arg or '').lower() for arg in (proc.info['cmdline'] or []))):
                    self.target_pid = proc.info['pid']
                    self.target_process = psutil.Process(self.target_pid)
                    print(f"Proceso encontrado: {proc.info['name']} (PID: {self.target_pid})")
                    return
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        print("No se pudo encontrar proceso Neo4j específico. Se monitoreará el consumo total del sistema.")

class Neo4jBenchmark:
    def __init__(self, uri, user, password):
        """Inicializa la conexión a Neo4j y el monitor de recursos."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.results = []
        self.resource_monitor = ResourceMonitor()  # <-- NUEVO: Monitor de recursos
        print("Benchmark conectado a Neo4j")
    
    def close(self):
        """Cierra la conexión"""
        self.driver.close()
        print("🔌 Conexión cerrada")
    
    def get_system_resources(self):
        """
        Obtiene el uso actual de recursos del sistema (CPU y RAM).
        
        Returns:
            dict: Diccionario con 'cpu_percent' y 'memory_percent'
        """
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)  # Pequeño intervalo para medición
            memory_info = psutil.virtual_memory()
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_info.percent,
                'memory_used_mb': memory_info.used / (1024 ** 2),  # MB
                'memory_available_mb': memory_info.available / (1024 ** 2)  # MB
            }
        except Exception as e:
            print(f"Error al obtener recursos del sistema: {e}")
            return {'cpu_percent': 0, 'memory_percent': 0, 'memory_used_mb': 0, 'memory_available_mb': 0}
    
    def get_database_size(self):
        """
        Obtiene el tamaño aproximado de la base de datos Neo4j en megabytes.
        
        Returns:
            float: Tamaño de la base de datos en MB
        """
        try:
            # Método 1: Usar procedimiento interno de Neo4j (disponible en versiones recientes)
            result = self._execute_query("CALL db.info() YIELD storageBytes RETURN storageBytes")
            if result and 'storageBytes' in result[0]:
                size_bytes = result[0]['storageBytes']
                return size_bytes / (1024 ** 2)  # Convertir a MB
        except Exception as e:
            # Método 2: Usar consulta alternativa
            try:
                result = self._execute_query("MATCH (n) RETURN sum(size(keys(n))) + sum(size([k in keys(n) | size(n[k])])) as estimatedSize")
                if result and 'estimatedSize' in result[0]:
                    estimated_bytes = result[0]['estimatedSize'] * 2  # Factor aproximado
                    return estimated_bytes / (1024 ** 2)
            except Exception:
                pass
        
        # Método 3: Estimar basado en número de nodos (muy aproximado)
        try:
            result = self._execute_query("MATCH (n) RETURN count(n) as nodeCount")
            if result and 'nodeCount' in result[0]:
                # Estimación: ~1KB por nodo en promedio (ajustable)
                estimated_size_mb = (result[0]['nodeCount'] * 1024) / (1024 ** 2)
                return estimated_size_mb
        except Exception:
            pass
        
        return 0.0
    
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
        """
        Ejecuta una consulta múltiples veces y captura métricas detalladas
        de TIEMPO y RECURSOS (CPU, RAM).
        
        Args:
            query_name: Nombre identificativo de la consulta
            query_template: Consulta Cypher a ejecutar
            params: Parámetros para la consulta
            iterations: Número de iteraciones a ejecutar
        
        Returns:
            dict: Estadísticas de tiempo y recursos
        """
        print(f"Ejecutando {query_name} ({iterations} iteraciones)...")
        
        times = []
        cpu_samples = []
        memory_samples = []
        params = params or {}
        
        for i in range(iterations):
            # Tomar muestra de recursos ANTES de la ejecución
            resources_before = self.get_system_resources()
            
            # Medir tiempo de ejecución
            start_time = time.perf_counter()
            
            try:
                self._execute_query(query_template, params)
            except Exception as e:
                print(f"Error en iteración {i+1}: {str(e)[:100]}...")
                raise
            
            end_time = time.perf_counter()
            
            # Tomar muestra de recursos DESPUÉS de la ejecución
            time.sleep(0.05)  # Pequeña pausa para estabilizar medición
            resources_after = self.get_system_resources()
            
            # Calcular métricas
            elapsed_ms = (end_time - start_time) * 1000
            times.append(elapsed_ms)
            
            # Calcular uso de recursos durante esta iteración
            cpu_delta = resources_after['cpu_percent'] - resources_before['cpu_percent']
            memory_delta = resources_after['memory_used_mb'] - resources_before['memory_used_mb']
            
            cpu_samples.append(max(0, cpu_delta))  # Evitar valores negativos
            memory_samples.append(memory_delta)
        
        # Calcular estadísticas de TIEMPO (igual que antes)
        time_stats = {
            'query': query_name,
            'iterations': iterations,
            'min': min(times),
            'max': max(times),
            'avg': statistics.mean(times),
            'median': statistics.median(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        }
        
        # Calcular estadísticas de RECURSOS (NUEVO)
        if cpu_samples and memory_samples:
            resource_stats = {
                'cpu_avg': statistics.mean(cpu_samples),
                'cpu_max': max(cpu_samples),
                'cpu_std_dev': statistics.stdev(cpu_samples) if len(cpu_samples) > 1 else 0,
                'memory_avg_mb': statistics.mean(memory_samples),
                'memory_max_mb': max(memory_samples),
                'memory_std_dev_mb': statistics.stdev(memory_samples) if len(memory_samples) > 1 else 0
            }
        else:
            resource_stats = {
                'cpu_avg': 0, 'cpu_max': 0, 'cpu_std_dev': 0,
                'memory_avg_mb': 0, 'memory_max_mb': 0, 'memory_std_dev_mb': 0
            }
        
        # Combinar estadísticas
        combined_stats = {**time_stats, **resource_stats}
        self.results.append(combined_stats)
        
        # Mostrar resumen combinado
        print(f"Tiempo Avg: {combined_stats['avg']:.2f}ms | "
              f"Min: {combined_stats['min']:.2f}ms | Max: {combined_stats['max']:.2f}ms")
        print(f"CPU Avg: {combined_stats['cpu_avg']:.1f}% | "
              f"Memoria Avg: {combined_stats['memory_avg_mb']:.1f} MB")
        
        return combined_stats
    
    def run_full_benchmark(self):
        """Ejecuta el benchmark completo con las 8 consultas definidas"""
        print("\nINICIANDO BENCHMARK COMPLETO")
        print("=" * 60)
        
        # Mostrar estado inicial del sistema
        initial_resources = self.get_system_resources()
        print(f"Estado inicial del sistema:")
        print(f"   CPU: {initial_resources['cpu_percent']:.1f}% | "
              f"Memoria usada: {initial_resources['memory_used_mb']:.1f} MB | "
              f"Disponible: {initial_resources['memory_available_mb']:.1f} MB")
        
        # Medir tamaño inicial de la base de datos
        initial_db_size = self.get_database_size()
        print(f"Tamaño inicial BD: {initial_db_size:.2f} MB")
        
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
                    'min': 0, 'max': 0, 'avg': 0, 'median': 0, 'std_dev': 0,
                    'cpu_avg': 0, 'cpu_max': 0, 'cpu_std_dev': 0,
                    'memory_avg_mb': 0, 'memory_max_mb': 0, 'memory_std_dev_mb': 0,
                    'error': str(e)[:200]
                })
            
            # Pequeña pausa entre consultas diferentes
            if i < len(queries) - 1:
                time.sleep(0.5)
        
        # Medir tamaño final de la base de datos
        final_db_size = self.get_database_size()
        print(f"\nTamaño final BD: {final_db_size:.2f} MB")
        print(f"Crecimiento durante benchmark: {final_db_size - initial_db_size:.2f} MB")
        
        # Mostrar estado final del sistema
        final_resources = self.get_system_resources()
        print(f"Estado final del sistema:")
        print(f"   CPU: {final_resources['cpu_percent']:.1f}% | "
              f"Memoria usada: {final_resources['memory_used_mb']:.1f} MB | "
              f"Disponible: {final_resources['memory_available_mb']:.1f} MB")
        
        print("\n" + "=" * 60)
        print("BENCHMARK COMPLETADO")
        print("=" * 60)
    
    def save_results(self, filename="resultados_benchmark_completo_10k.csv"):
        """
        Guarda los resultados COMPLETOS (tiempo + recursos) en un archivo CSV.
        
        Args:
            filename: Nombre del archivo CSV de salida
        """
        if not self.results:
            print("No hay resultados para guardar")
            return
        
        # Definir campos completos (tiempo + recursos)
        fieldnames = [
            'query', 'iterations', 
            'min', 'max', 'avg', 'median', 'std_dev',  # Métricas de tiempo
            'cpu_avg', 'cpu_max', 'cpu_std_dev',        # Métricas de CPU
            'memory_avg_mb', 'memory_max_mb', 'memory_std_dev_mb'  # Métricas de memoria
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in self.results:
                # Filtrar solo los campos definidos y redondear valores decimales
                row = {}
                for field in fieldnames:
                    if field in result:
                        value = result[field]
                        # Redondear valores float para mejor legibilidad
                        if isinstance(value, float):
                            row[field] = round(value, 4)
                        else:
                            row[field] = value
                    else:
                        row[field] = ''
                writer.writerow(row)
        
        print(f"Resultados completos guardados en: {filename}")
        print(f"Total de consultas evaluadas: {len([r for r in self.results if r.get('iterations', 0) > 0])}")
    
    def print_summary(self):
        """Imprime un resumen COMPLETO de los resultados en consola"""
        valid_results = [r for r in self.results if r.get('iterations', 0) > 0]
        
        if not valid_results:
            print("No hay resultados válidos para mostrar")
            return
        
        print("\n" + "=" * 80)
        print("RESUMEN COMPLETO DE RESULTADOS (TIEMPO + RECURSOS)")
        print("=" * 80)
        print(f"{'Consulta':<25} {'Tiempo (ms)':<20} {'CPU (%)':<15} {'Memoria (MB)':<15}")
        print(f"{'':<25} {'Avg':<10} {'Min':<10} {'Avg':<7} {'Max':<8} {'Avg':<7} {'Max':<8}")
        print("-" * 80)
        
        for result in valid_results:
            print(f"{result['query']:<25} "
                  f"{result['avg']:<10.2f} {result['min']:<10.2f} "
                  f"{result.get('cpu_avg', 0):<7.1f} {result.get('cpu_max', 0):<8.1f} "
                  f"{result.get('memory_avg_mb', 0):<7.1f} {result.get('memory_max_mb', 0):<8.1f}")

# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    # CONFIGURACIÓN (AJUSTA SEGÚN TU ENTORNO)
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "admin123"  # <-- ¡CAMBIAR POR TU CONTRASEÑA REAL!
    
    print("=" * 60)
    print("BENCHMARK COMPLETO - NEO4J")
    print("Medición de Rendimiento y Recursos (CPU, RAM, Disco)")
    print("Proyecto de Maestría en Ingeniería de Software")
    print("=" * 60)
    
    # Verificar que psutil esté instalado
    try:
        import psutil
        print("Módulo psutil disponible para monitoreo de recursos")
    except ImportError:
        print("ERROR: El módulo 'psutil' no está instalado.")
        print("   Instálalo ejecutando: pip install psutil")
        exit(1)
    
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