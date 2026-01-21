import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import style

# Configuración de estilo
style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.figsize'] = [14, 8]
plt.rcParams['font.size'] = 11

# Datos corregidos
consultas = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8']
tiempos_100 = [11.39, 10.92, 10.01, 20.83, 22.50, 14.08, 52.98, 14.30]
tiempos_10k = [22.06, 13.83, 14.86, 76.96, 58.13, 12.93, 57.67, 11.56]
tiempos_100k = [28.49, 17.28, 20.08, 236.31, 182.97, 14.34, 38.66, 11.90]

cpu_100 = [0.99, 0.40, 1.83, 1.75, 0.91, 0.11, 1.24, 0.39]
cpu_10k = [0.42, 0.28, 1.63, 4.10, 2.09, 0.45, 0.87, 0.17]
cpu_100k = [4.26, 1.04, 1.77, 2.78, 0.59, 2.55, 1.08, 1.25]

mem_100 = [0.61, -0.74, -0.74, 0.09, 0.11, 0.27, 0.36, 0.01]
mem_10k = [-2.87, -0.59, 0.05, 0.14, 6.64, -0.12, 0.06, -0.19]
mem_100k = [1.51, -9.49, 4.09, 9.82, -13.82, 0.01, 0.21, 0.19]

# Datos de generación
escalas = ['100', '10k', '100k']
usuarios = [100, 10000, 100000]
tiempos_gen = [6.84, 615.33, 45418.73]
nodos = [447, 40505, 404794]
relaciones = [1024, 105297, 1050265]
espacio_disco = [1.40, 15.9, 215]

# 1. GRÁFICO DE TIEMPOS DE EJECUCIÓN (original)
fig1, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

x = np.arange(len(consultas))
width = 0.25

# Subgráfico 1: Tiempos absolutos
bars1 = ax1.bar(x - width, tiempos_100, width, label='100 usuarios', alpha=0.8)
bars2 = ax1.bar(x, tiempos_10k, width, label='10k usuarios', alpha=0.8)
bars3 = ax1.bar(x + width, tiempos_100k, width, label='100k usuarios', alpha=0.8)

ax1.set_xlabel('Consultas')
ax1.set_ylabel('Tiempo (ms)')
ax1.set_title('Tiempo de Ejecución por Consulta y Escala')
ax1.set_xticks(x)
ax1.set_xticklabels(consultas)
ax1.legend()
ax1.grid(True, alpha=0.3)

for bars in [bars1, bars2, bars3]:
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height + 5,
                f'{height:.1f}', ha='center', va='bottom', fontsize=8)

# Subgráfico 2: Factor de escalabilidad
factor_100k_100 = [t100k/t100 if t100 > 0 else 0 for t100, t100k in zip(tiempos_100, tiempos_100k)]
factor_10k_100 = [t10k/t100 if t100 > 0 else 0 for t100, t10k in zip(tiempos_100, tiempos_10k)]

ax2.plot(consultas, factor_100k_100, 'o-', linewidth=2, markersize=8, label='100k/100', color='red')
ax2.plot(consultas, factor_10k_100, 's--', linewidth=2, markersize=6, label='10k/100', color='blue')
ax2.axhline(y=1, color='gray', linestyle=':', alpha=0.5)
ax2.set_xlabel('Consultas')
ax2.set_ylabel('Factor de Escalabilidad')
ax2.set_title('Factor de Aumento de Tiempo (vs 100 usuarios)')
ax2.legend()
ax2.grid(True, alpha=0.3)
ax2.set_ylim(0, max(factor_100k_100)*1.1)

plt.tight_layout()
plt.savefig('1_tiempos_ejecucion.png', dpi=300, bbox_inches='tight')
plt.show()

# 2. GRÁFICO DE USO DE RECURSOS (CPU y Memoria) (original)
fig2, axes = plt.subplots(2, 2, figsize=(15, 10))

# CPU por escala
for ax, cpu_data, escala, color in zip(axes[0], 
                                       [cpu_100, cpu_10k, cpu_100k], 
                                       ['100 usuarios', '10k usuarios', '100k usuarios'],
                                       ['blue', 'green', 'red']):
    bars = ax.bar(consultas, cpu_data, color=color, alpha=0.7)
    ax.set_title(f'Uso de CPU - {escala}')
    ax.set_ylabel('CPU (%)')
    ax.set_ylim(0, max(cpu_100k)*1.2)
    ax.grid(True, alpha=0.3)
    
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{height:.2f}', ha='center', va='bottom', fontsize=8)

# Memoria por escala
for ax, mem_data, escala, color in zip(axes[1], 
                                       [mem_100, mem_10k, mem_100k], 
                                       ['100 usuarios', '10k usuarios', '100k usuarios'],
                                       ['purple', 'orange', 'brown']):
    bars = ax.bar(consultas, mem_data, color=color, alpha=0.7)
    ax.set_title(f'Cambio de Memoria - {escala}')
    ax.set_ylabel('Δ Memoria (MB)')
    ax.axhline(y=0, color='black', linewidth=0.5)
    ax.grid(True, alpha=0.3)
    
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + (0.3 if height >= 0 else -0.5),
                f'{height:.2f}', ha='center', va='bottom' if height >= 0 else 'top', fontsize=8)

plt.tight_layout()
plt.savefig('2_uso_recursos.png', dpi=300, bbox_inches='tight')
plt.show()

# 3. GRÁFICO DE ESPACIO EN DISCO Y EFICIENCIA (original)
fig3, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Espacio total
ax1.plot(escalas, espacio_disco, 'o-', linewidth=2, markersize=10, color='darkgreen')
ax1.set_xlabel('Escala (usuarios)')
ax1.set_ylabel('Espacio en Disco (MB)')
ax1.set_title('Crecimiento del Espacio en Disco')
ax1.grid(True, alpha=0.3)
ax1.set_yscale('log')

for i, (esc, esp) in enumerate(zip(escalas, espacio_disco)):
    ax1.text(i, esp*1.2, f'{esp} MB', ha='center', va='bottom', fontweight='bold')

# Eficiencia (KB por usuario)
eficiencia = [(esp*1024)/usu for esp, usu in zip(espacio_disco, usuarios)]

bars = ax2.bar(escalas, eficiencia, color='teal', alpha=0.7)
ax2.set_xlabel('Escala (usuarios)')
ax2.set_ylabel('KB por Usuario')
ax2.set_title('Eficiencia de Almacenamiento')
ax2.grid(True, alpha=0.3)

for bar, eff in zip(bars, eficiencia):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height + 0.2,
            f'{eff:.1f} KB', ha='center', va='bottom')

plt.tight_layout()
plt.savefig('3_espacio_disco.png', dpi=300, bbox_inches='tight')
plt.show()

# 4. GRÁFICO DE COMPARATIVA POR TIPO DE CONSULTA (original)
fig4, ax = plt.subplots(figsize=(12, 7))

# Agrupar consultas por tipo
tipos = {
    'CRUD Simple': ['Q1', 'Q2', 'Q8'],
    'Filtro/Agregación': ['Q3', 'Q5'],
    'JOIN/Relacional': ['Q4'],
    'Grafo': ['Q7'],
    'Mantenimiento': ['Q6']
}

# Calcular promedios por tipo
promedios_100 = []
promedios_100k = []
labels = []

for tipo, consultas_tipo in tipos.items():
    indices = [consultas.index(q) for q in consultas_tipo]
    if indices:
        avg_100 = np.mean([tiempos_100[i] for i in indices])
        avg_100k = np.mean([tiempos_100k[i] for i in indices])
        promedios_100.append(avg_100)
        promedios_100k.append(avg_100k)
        labels.append(tipo)

x = np.arange(len(labels))
width = 0.35

bars1 = ax.bar(x - width/2, promedios_100, width, label='100 usuarios', color='blue', alpha=0.7)
bars2 = ax.bar(x + width/2, promedios_100k, width, label='100k usuarios', color='red', alpha=0.7)

ax.set_xlabel('Tipo de Consulta')
ax.set_ylabel('Tiempo Promedio (ms)')
ax.set_title('Comparativa de Rendimiento por Tipo de Consulta')
ax.set_xticks(x)
ax.set_xticklabels(labels, rotation=15)
ax.legend()
ax.grid(True, alpha=0.3)

# Añadir ratio
for i, (p100, p100k) in enumerate(zip(promedios_100, promedios_100k)):
    ratio = p100k/p100 if p100 > 0 else 0
    ax.text(i, max(p100, p100k) + 5, f'{ratio:.1f}x', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('4_comparativa_tipos.png', dpi=300, bbox_inches='tight')
plt.show()

# 5. NUEVO GRÁFICO: ANÁLISIS DE GENERACIÓN
fig5, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))

# Tiempo total (escala logarítmica)
ax1.semilogy(escalas, tiempos_gen, 'o-', linewidth=3, markersize=12, color='darkred')
ax1.set_xlabel('Escala (usuarios)')
ax1.set_ylabel('Tiempo (segundos, escala log)')
ax1.set_title('Tiempo Total de Generación de Datos')
ax1.grid(True, alpha=0.3, which='both')
for i, (esc, t) in enumerate(zip(escalas, tiempos_gen)):
    if i == 2:  # Para 100k, poner etiqueta arriba
        ax1.text(i, t/2, f'{t/3600:.1f}h\n({t:.0f}s)', ha='center', va='center', 
                fontweight='bold', bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
    else:
        ax1.text(i, t*1.2, f'{t:.1f}s', ha='center', va='bottom', fontweight='bold')

# Tiempo por usuario
tiempo_por_usuario = [t/u for t, u in zip(tiempos_gen, usuarios)]
ax2.bar(escalas, tiempo_por_usuario, color=['green', 'yellow', 'red'], alpha=0.7)
ax2.set_xlabel('Escala (usuarios)')
ax2.set_ylabel('Segundos por Usuario')
ax2.set_title('Eficiencia de Generación por Usuario')
ax2.grid(True, alpha=0.3, axis='y')
for i, (esc, tpu) in enumerate(zip(escalas, tiempo_por_usuario)):
    color = 'black' if i != 2 else 'white'
    ax2.text(i, tpu*1.05, f'{tpu:.4f}s', ha='center', va='bottom', 
            fontweight='bold', color=color)

# Crecimiento de nodos y relaciones
x = np.arange(len(escalas))
width = 0.35
rects1 = ax3.bar(x - width/2, nodos, width, label='Nodos', color='blue', alpha=0.7)
rects2 = ax3.bar(x + width/2, relaciones, width, label='Relaciones', color='purple', alpha=0.7)
ax3.set_xlabel('Escala (usuarios)')
ax3.set_ylabel('Cantidad (escala log)')
ax3.set_title('Crecimiento de Nodos y Relaciones')
ax3.set_xticks(x)
ax3.set_xticklabels(escalas)
ax3.set_yscale('log')
ax3.legend()
ax3.grid(True, alpha=0.3, axis='y')

# Densidad del grafo
densidad = [r/n for r, n in zip(relaciones, nodos)]
ax4.plot(escalas, densidad, 's-', linewidth=2, markersize=10, color='darkgreen')
ax4.set_xlabel('Escala (usuarios)')
ax4.set_ylabel('Relaciones por Nodo')
ax4.set_title('Densidad del Grafo (Relaciones/Nodo)')
ax4.grid(True, alpha=0.3)
for i, (esc, dens) in enumerate(zip(escalas, densidad)):
    ax4.text(i, dens*1.05, f'{dens:.2f}', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('5_analisis_generacion.png', dpi=300, bbox_inches='tight')
plt.show()

# 6. NUEVOS GRÁFICOS: ANÁLISIS DE RECURSOS DEL SISTEMA
fig6, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))

# ==================== GRÁFICO 1: ESPACIO EN DISCO DETALLADO ====================
# Espacio total y crecimiento relativo
ax1.plot(escalas, espacio_disco, 's-', linewidth=3, markersize=12, 
         color='darkblue', label='Espacio total')
ax1.fill_between(escalas, 0, espacio_disco, alpha=0.2, color='blue')
ax1.set_xlabel('Escala (usuarios)', fontweight='bold')
ax1.set_ylabel('Espacio en Disco (MB)', fontweight='bold')
ax1.set_title('Consumo de Espacio en Disco por Escala', fontweight='bold', fontsize=14)
ax1.grid(True, alpha=0.3)
ax1.set_yscale('log')

# Añadir etiquetas con crecimiento
for i, (esc, esp) in enumerate(zip(escalas, espacio_disco)):
    if i > 0:
        crecimiento = espacio_disco[i] / espacio_disco[i-1]
        ax1.text(i, esp*1.3, f'{crecimiento:.1f}x\ncrecimiento', 
                ha='center', va='bottom', fontsize=9, 
                bbox=dict(boxstyle="round,pad=0.2", facecolor="yellow", alpha=0.7))
    ax1.text(i, esp*0.5, f'{esp:.1f} MB', ha='center', va='center', 
            fontweight='bold', color='white')

# ==================== GRÁFICO 2: USO DE CPU PROMEDIO ====================
# Calcular CPU promedio por escala
cpu_prom_100 = np.mean(cpu_100)
cpu_prom_10k = np.mean(cpu_10k)
cpu_prom_100k = np.mean(cpu_100k)
cpu_promedios = [cpu_prom_100, cpu_prom_10k, cpu_prom_100k]

# También calcular CPU máxima por escala
cpu_max_100 = max(cpu_100)
cpu_max_10k = max(cpu_10k)
cpu_max_100k = max(cpu_100k)
cpu_maximos = [cpu_max_100, cpu_max_10k, cpu_max_100k]

# Gráfico de barras agrupadas
x = np.arange(len(escalas))
width = 0.35
bars1 = ax2.bar(x - width/2, cpu_promedios, width, label='Promedio', color='green', alpha=0.7)
bars2 = ax2.bar(x + width/2, cpu_maximos, width, label='Máximo', color='red', alpha=0.7)

ax2.set_xlabel('Escala (usuarios)', fontweight='bold')
ax2.set_ylabel('Uso de CPU (%)', fontweight='bold')
ax2.set_title('Consumo de CPU por Escala', fontweight='bold', fontsize=14)
ax2.set_xticks(x)
ax2.set_xticklabels(escalas)
ax2.legend()
ax2.grid(True, alpha=0.3, axis='y')

# Añadir valores
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                f'{height:.2f}%', ha='center', va='bottom', fontsize=9)

# ==================== GRÁFICO 3: USO DE MEMORIA PROMEDIO ====================
# Calcular memoria promedio (valor absoluto) por escala
mem_abs_100 = np.mean([abs(m) for m in mem_100])
mem_abs_10k = np.mean([abs(m) for m in mem_10k])
mem_abs_100k = np.mean([abs(m) for m in mem_100k])
mem_prom_abs = [mem_abs_100, mem_abs_10k, mem_abs_100k]

# Calcular memoria máxima (valor absoluto)
mem_max_100 = max([abs(m) for m in mem_100])
mem_max_10k = max([abs(m) for m in mem_10k])
mem_max_100k = max([abs(m) for m in mem_100k])
mem_max_abs = [mem_max_100, mem_max_10k, mem_max_100k]

# Gráfico de barras
bars3 = ax3.bar(x - width/2, mem_prom_abs, width, label='Promedio (abs)', color='purple', alpha=0.7)
bars4 = ax3.bar(x + width/2, mem_max_abs, width, label='Máximo (abs)', color='orange', alpha=0.7)

ax3.set_xlabel('Escala (usuarios)', fontweight='bold')
ax3.set_ylabel('Cambio de Memoria (MB)', fontweight='bold')
ax3.set_title('Consumo de Memoria por Escala (Valor Absoluto)', fontweight='bold', fontsize=14)
ax3.set_xticks(x)
ax3.set_xticklabels(escalas)
ax3.legend()
ax3.grid(True, alpha=0.3, axis='y')

# Añadir valores
for bars in [bars3, bars4]:
    for bar in bars:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                f'{height:.2f} MB', ha='center', va='bottom', fontsize=9)

# ==================== GRÁFICO 4: COMPARATIVA DE RECURSOS (normalizado) ====================
# Normalizar todos los recursos a escala 0-1
def normalizar(datos):
    max_val = max(datos)
    return [d/max_val for d in datos]

espacio_norm = normalizar(espacio_disco)
cpu_norm = normalizar(cpu_promedios)
mem_norm = normalizar(mem_prom_abs)

# Gráfico de líneas comparativo
ax4.plot(escalas, espacio_norm, 'o-', linewidth=2, markersize=10, 
         label='Espacio en disco', color='blue')
ax4.plot(escalas, cpu_norm, 's-', linewidth=2, markersize=8, 
         label='CPU', color='green')
ax4.plot(escalas, mem_norm, '^-', linewidth=2, markersize=8, 
         label='Memoria', color='purple')

ax4.set_xlabel('Escala (usuarios)', fontweight='bold')
ax4.set_ylabel('Uso Relativo (normalizado)', fontweight='bold')
ax4.set_title('Comparativa de Recursos (Normalizado al Máximo)', fontweight='bold', fontsize=14)
ax4.legend()
ax4.grid(True, alpha=0.3)

# Añadir etiquetas de valores reales
for i, esc in enumerate(escalas):
    ax4.text(i, espacio_norm[i] + 0.05, f'{espacio_disco[i]:.1f}MB', 
            ha='center', va='bottom', fontsize=8, color='blue')
    ax4.text(i, cpu_norm[i] + 0.03, f'{cpu_promedios[i]:.2f}%', 
            ha='center', va='bottom', fontsize=8, color='green')
    ax4.text(i, mem_norm[i] - 0.05, f'{mem_prom_abs[i]:.2f}MB', 
            ha='center', va='top', fontsize=8, color='purple')

plt.tight_layout()
plt.savefig('6_recursos_sistema.png', dpi=300, bbox_inches='tight')
plt.show()

# 7. GRÁFICO ADICIONAL: RESUMEN DE METRICAS CLAVE
fig7, axes = plt.subplots(2, 3, figsize=(18, 10))

# Preparar métricas clave
metricas = {
    'Tiempo Generación (s)': tiempos_gen,
    'Espacio Disco (MB)': espacio_disco,
    'CPU Promedio (%)': cpu_promedios,
    'Memoria Prom (MB)': mem_prom_abs,
    'Nodos Totales': nodos,
    'Relaciones Totales': relaciones
}

colores = ['red', 'blue', 'green', 'purple', 'orange', 'brown']

for idx, (titulo, datos) in enumerate(metricas.items()):
    ax = axes[idx//3, idx%3]
    
    if 'Tiempo' in titulo or 'Espacio' in titulo or 'Nodos' in titulo or 'Relaciones' in titulo:
        # Usar escala logarítmica para estas métricas
        ax.semilogy(escalas, datos, 'o-', linewidth=2, markersize=8, color=colores[idx])
        ax.set_ylabel(titulo + ' (log)', fontweight='bold')
    else:
        ax.plot(escalas, datos, 's-', linewidth=2, markersize=8, color=colores[idx])
        ax.set_ylabel(titulo, fontweight='bold')
    
    ax.set_xlabel('Escala (usuarios)', fontweight='bold')
    ax.set_title(titulo, fontweight='bold', fontsize=12)
    ax.grid(True, alpha=0.3)
    
    # Añadir valores
    for i, (esc, val) in enumerate(zip(escalas, datos)):
        if 'Tiempo' in titulo and i == 2:  # Para tiempo de 100k
            ax.text(i, val/2, f'{val/3600:.1f}h', ha='center', va='center',
                   fontweight='bold', bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.8))
        else:
            if isinstance(val, float):
                text = f'{val:.2f}' if val < 1000 else f'{val:.0f}'
            else:
                text = f'{val:,}'.replace(',', '.')
            ax.text(i, val*1.05, text, ha='center', va='bottom', fontweight='bold')

plt.suptitle('Resumen de Métricas Clave por Escala', fontweight='bold', fontsize=16, y=1.02)
plt.tight_layout()
plt.savefig('7_resumen_metricas.png', dpi=300, bbox_inches='tight')
plt.show()

print("="*60)
print("GRÁFICOS GENERADOS EXITOSAMENTE")
print("="*60)
print("\n1. 1_tiempos_ejecucion.png - Tiempos de ejecución por consulta")
print("2. 2_uso_recursos.png - Uso de CPU y memoria por consulta")
print("3. 3_espacio_disco.png - Espacio en disco y eficiencia")
print("4. 4_comparativa_tipos.png - Comparativa por tipo de consulta")
print("5. 5_analisis_generacion.png - Análisis de generación de datos")
print("6. 6_recursos_sistema.png - NUEVO: Análisis detallado de recursos")
print("7. 7_resumen_metricas.png - NUEVO: Resumen de métricas clave")
print("\nTodos los gráficos se han guardado en el directorio actual.")