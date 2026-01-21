import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import style

# Configuración
style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.figsize'] = [14, 10]
plt.rcParams['font.size'] = 11

# Datos
escalas = ['100', '10k', '100k']
usuarios = [100, 10000, 100000]
tiempos_gen = [6.84, 615.33, 45418.73]
nodos = [447, 40505, 404794]
relaciones = [1024, 105297, 1050265]

# 1. GRÁFICO DE TIEMPOS DE GENERACIÓN
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))

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
plt.savefig('analisis_generacion.png', dpi=300, bbox_inches='tight')
plt.show()

# 2. GRÁFICO COMPARATIVO: GENERACIÓN vs CONSULTAS
fig2, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Datos de consultas (tiempos promedio)
tiempos_q1 = [11.39, 22.06, 28.49]  # Inserción (similar a generación)
tiempos_q4 = [20.83, 76.96, 236.31]  # JOIN (operación compleja)

# Factor de crecimiento vs 100 usuarios
factor_gen = [1, tiempos_gen[1]/tiempos_gen[0], tiempos_gen[2]/tiempos_gen[0]]
factor_q1 = [1, tiempos_q1[1]/tiempos_q1[0], tiempos_q1[2]/tiempos_q1[0]]
factor_q4 = [1, tiempos_q4[1]/tiempos_q4[0], tiempos_q4[2]/tiempos_q4[0]]

# Comparación lineal
ax1.plot(escalas, factor_gen, 'o--', linewidth=2, markersize=10, 
        label='Generación (Inserción Masiva)', color='red')
ax1.plot(escalas, factor_q1, 's--', linewidth=2, markersize=8, 
        label='Q1 (Inserción Simple)', color='blue')
ax1.plot(escalas, factor_q4, '^--', linewidth=2, markersize=8, 
        label='Q4 (JOIN Simple)', color='green')
ax1.set_xlabel('Escala (usuarios)')
ax1.set_ylabel('Factor de Crecimiento (vs 100 usuarios)')
ax1.set_title('Comparación de Escalabilidad: Inserción vs Consultas')
ax1.legend()
ax1.grid(True, alpha=0.3)
ax1.set_yscale('log')

# Eficiencia relativa
eficiencia_gen = [100/t for t in tiempos_gen]  # inverso del tiempo (normalizado)
eficiencia_q1 = [100/t for t in tiempos_q1]
eficiencia_q4 = [100/t for t in tiempos_q4]

# Normalizar al valor de 100 usuarios
norm_gen = [e/eficiencia_gen[0] for e in eficiencia_gen]
norm_q1 = [e/eficiencia_q1[0] for e in eficiencia_q1]
norm_q4 = [e/eficiencia_q4[0] for e in eficiencia_q4]

x_pos = np.arange(len(escalas))
width = 0.25
ax2.bar(x_pos - width, norm_gen, width, label='Generación', color='red', alpha=0.7)
ax2.bar(x_pos, norm_q1, width, label='Inserción Simple', color='blue', alpha=0.7)
ax2.bar(x_pos + width, norm_q4, width, label='JOIN Simple', color='green', alpha=0.7)
ax2.set_xlabel('Escala (usuarios)')
ax2.set_ylabel('Eficiencia Relativa (vs 100 usuarios)')
ax2.set_title('Eficiencia Relativa por Operación')
ax2.set_xticks(x_pos)
ax2.set_xticklabels(escalas)
ax2.legend()
ax2.grid(True, alpha=0.3, axis='y')
ax2.axhline(y=1, color='gray', linestyle=':', linewidth=1)

plt.tight_layout()
plt.savefig('comparativa_generacion_consultas.png', dpi=300, bbox_inches='tight')
plt.show()

# 3. GRÁFICO DE ESTRUCTURA DE LA BASE
fig3, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Distribución de nodos por tipo (estimada)
# Asumimos: Usuarios = escala, Productos = 0.5× escala, Pedidos = resto
usuarios_nodos = usuarios
productos_nodos = [50, 500, 5000]
pedidos_nodos = [n - u - p for n, u, p in zip(nodos, usuarios_nodos, productos_nodos)]

# Gráfico de torta para 100k (escala más interesante)
labels = ['Usuarios', 'Productos', 'Pedidos']
sizes = [usuarios_nodos[2], productos_nodos[2], pedidos_nodos[2]]
colors = ['#ff9999', '#66b3ff', '#99ff99']
explode = (0.05, 0.05, 0.05)

ax1.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
        startangle=90, shadow=True)
ax1.set_title('Distribución de Nodos (100k usuarios)')
ax1.axis('equal')

# Relaciones por tipo (estimación)
# Cada pedido tiene ~3.5 relaciones, cada usuario tiene ~10.5 relaciones
rel_pedidos = [r * 0.75 for r in relaciones]  # Estimado: 75% relaciones son de pedidos
rel_usuarios = [r * 0.25 for r in relaciones]  # 25% relaciones son de usuarios (amigos, etc.)

ax2.bar(escalas, rel_pedidos, width=0.5, label='Relaciones Pedidos', 
       color='orange', alpha=0.7, bottom=rel_usuarios)
ax2.bar(escalas, rel_usuarios, width=0.5, label='Relaciones Usuarios', 
       color='lightblue', alpha=0.7)
ax2.set_xlabel('Escala (usuarios)')
ax2.set_ylabel('Cantidad de Relaciones')
ax2.set_title('Distribución de Relaciones por Tipo')
ax2.set_yscale('log')
ax2.legend()
ax2.grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.savefig('estructura_base.png', dpi=300, bbox_inches='tight')
plt.show()

# 4. GRÁFICO DE ANÁLISIS DE RENDIMIENTO COMPUESTO
fig4, ax = plt.subplots(figsize=(12, 8))

# Preparar datos para gráfico de radar (spider chart)
categories = ['Tiempo Generación', 'Tamaño Disco', 'Rendimiento Q1', 
              'Rendimiento Q4', 'Rendimiento Q7', 'Uso CPU']

# Normalizar datos (0-1, donde 1 es mejor)
# Invertir tiempos (más tiempo = peor rendimiento)
norm_gen = [1 - min(t/1000, 1) for t in [6.84, 615.33, 45418.73]]  # Capado a 1000s
norm_disco = [1.40/215, 15.9/215, 1]  # Tamaño relativo (invertido)
norm_q1 = [1, 0.8, 0.6]  # Rendimiento Q1 (estimado de datos)
norm_q4 = [1, 0.3, 0.1]  # Rendimiento Q4 (estimado de datos)
norm_q7 = [1, 0.9, 1.2]  # Rendimiento Q7 (mejora a 100k)
norm_cpu = [1, 0.8, 0.7]  # Uso CPU (menos = mejor)

angles = np.linspace(0, 2*np.pi, len(categories), endpoint=False).tolist()
angles += angles[:1]  # Cerrar el círculo

# Crear gráfico de radar
for i, escala in enumerate(escalas):
    values = [norm_gen[i], norm_disco[i], norm_q1[i], norm_q4[i], norm_q7[i], norm_cpu[i]]
    values += values[:1]  # Cerrar el círculo
    
    ax.plot(angles, values, 'o-', linewidth=2, label=f'{escala} usuarios')
    ax.fill(angles, values, alpha=0.1)

# Configurar ejes
ax.set_xticks(angles[:-1])
ax.set_xticklabels(categories)
ax.set_ylim(0, 1.3)
ax.set_yticks([0, 0.25, 0.5, 0.75, 1.0])
ax.set_yticklabels(['0', '25%', '50%', '75%', '100%'])
ax.set_title('Análisis de Rendimiento Compuesto por Escala\n(valores normalizados, mayor = mejor)')
ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
ax.grid(True)

plt.tight_layout()
plt.savefig('rendimiento_compuesto.png', dpi=300, bbox_inches='tight')
plt.show()

print("Gráficos generados:")
print("1. analisis_generacion.png")
print("2. comparativa_generacion_consultas.png")
print("3. estructura_base.png")
print("4. rendimiento_compuesto.png")