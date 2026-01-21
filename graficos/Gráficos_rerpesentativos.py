import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Datos para gráficos
escalas = ['100', '10k', '100k', '500k']

# Tiempos de generación (logarítmico)
tiempos_gen = {
    'Neo4j': [6.84, 615.33, 45418.73, None],
    'PostgreSQL': [0.30, 27.64, 280.72, 1445.62],
    'MongoDB': [0.07, 1.79, 24.93, 273.28]
}

# Espacio en disco
espacio = {
    'Neo4j': [1.40, 15.9, 215, None],
    'PostgreSQL': [8.52, 24.22, 168.24, 802.34],
    'MongoDB': [0.17, 19.04, 190.90, 956.22]
}

# Rendimiento consultas Q4 (JOIN simple) en 100k
rendimiento_100k = {
    'Q1': [28.49, 4.81, 4.45],
    'Q2': [17.28, 1.88, 2.43],
    'Q3': [20.08, 3.38, 52.79],
    'Q4': [236.31, 170.53, 282.16],
    'Q5': [182.97, 188.68, 438.47],
    'Q6': [14.34, 1.78, 210.05],
    'Q7': [38.66, 131.22, 291.05],
    'Q8': [11.90, 30.54, 131.40]
}

# 1. Gráfico de tiempos de generación (escala logarítmica)
fig, ax = plt.subplots(2, 2, figsize=(15, 12))

# Gráfico 1: Tiempos de generación
ax1 = ax[0, 0]
x = np.arange(len(escalas))
width = 0.25
colors = ['#FF6B6B', '#4ECDC4', '#45B7D1']

# Ajustar datos para Neo4j (None en 500k)
neo4j_adj = [t if t is not None else 0 for t in tiempos_gen['Neo4j']]

ax1.bar(x - width, neo4j_adj, width, label='Neo4j', color=colors[0], alpha=0.8)
ax1.bar(x, tiempos_gen['PostgreSQL'], width, label='PostgreSQL', color=colors[1], alpha=0.8)
ax1.bar(x + width, tiempos_gen['MongoDB'], width, label='MongoDB', color=colors[2], alpha=0.8)

ax1.set_yscale('log')
ax1.set_ylabel('Tiempo (segundos, log)', fontsize=10)
ax1.set_title('Tiempos de Generación de Datos', fontsize=12, fontweight='bold')
ax1.set_xticks(x)
ax1.set_xticklabels(escalas)
ax1.legend()
ax1.grid(True, alpha=0.3)

# Anotar valores
for i, v in enumerate(neo4j_adj):
    if v > 0:
        ax1.text(i - width, v*1.1, f'{v:.1f}', ha='center', fontsize=8)

for i, v in enumerate(tiempos_gen['PostgreSQL']):
    ax1.text(i, v*1.1, f'{v:.1f}', ha='center', fontsize=8)

for i, v in enumerate(tiempos_gen['MongoDB']):
    ax1.text(i + width, v*1.1, f'{v:.1f}', ha='center', fontsize=8)

# Gráfico 2: Espacio en disco
ax2 = ax[0, 1]
neo4j_espacio = [e if e is not None else 0 for e in espacio['Neo4j']]

ax2.bar(x - width, neo4j_espacio, width, label='Neo4j', color=colors[0], alpha=0.8)
ax2.bar(x, espacio['PostgreSQL'], width, label='PostgreSQL', color=colors[1], alpha=0.8)
ax2.bar(x + width, espacio['MongoDB'], width, label='MongoDB', color=colors[2], alpha=0.8)

ax2.set_ylabel('Espacio (MB)', fontsize=10)
ax2.set_title('Espacio en Disco por Escala', fontsize=12, fontweight='bold')
ax2.set_xticks(x)
ax2.set_xticklabels(escalas)
ax2.legend()
ax2.grid(True, alpha=0.3)

# Gráfico 3: Rendimiento de consultas (100k usuarios)
ax3 = ax[1, 0]
queries = list(rendimiento_100k.keys())
neo4j_q = [rendimiento_100k[q][0] for q in queries]
postgres_q = [rendimiento_100k[q][1] for q in queries]
mongo_q = [rendimiento_100k[q][2] for q in queries]

x_q = np.arange(len(queries))
width_q = 0.25

ax3.bar(x_q - width_q, neo4j_q, width_q, label='Neo4j', color=colors[0], alpha=0.8)
ax3.bar(x_q, postgres_q, width_q, label='PostgreSQL', color=colors[1], alpha=0.8)
ax3.bar(x_q + width_q, mongo_q, width_q, label='MongoDB', color=colors[2], alpha=0.8)

ax3.set_ylabel('Tiempo (ms)', fontsize=10)
ax3.set_title('Rendimiento de Consultas (100k usuarios)', fontsize=12, fontweight='bold')
ax3.set_xticks(x_q)
ax3.set_xticklabels(queries, rotation=45, ha='right')
ax3.legend()
ax3.grid(True, alpha=0.3)

# Gráfico 4: KB por usuario (escala 100k)
ax4 = ax[1, 1]
sistemas = ['Neo4j', 'PostgreSQL', 'MongoDB']
kb_usuario = [2.2, 1.7, 0.8]

bars = ax4.bar(sistemas, kb_usuario, color=colors, alpha=0.8)
ax4.set_ylabel('KB por usuario', fontsize=10)
ax4.set_title('Eficiencia de Almacenamiento (100k usuarios)', fontsize=12, fontweight='bold')
ax4.grid(True, alpha=0.3)

# Añadir valores encima de las barras
for bar, val in zip(bars, kb_usuario):
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height*1.05,
             f'{val:.1f} KB', ha='center', va='bottom', fontsize=9)

plt.tight_layout()
plt.savefig('comparativa_bases_datos.png', dpi=300, bbox_inches='tight')
plt.show()

# Gráfico de escalabilidad para consulta Q4 (JOIN simple)

fig, ax = plt.subplots(figsize=(10, 6))

# Datos Q4 por escala
q4_tiempos = {
    '100': [20.83, 3.52, 4.91],
    '10k': [76.96, 35.20, 49.05],
    '100k': [236.31, 170.53, 282.16],
    '500k': [None, 833.53, 1524.22]
}

escalas_q4 = ['100', '10k', '100k', '500k']
markers = ['o', 's', '^']

for idx, (db, color) in enumerate(zip(['Neo4j', 'PostgreSQL', 'MongoDB'], colors)):
    tiempos = []
    for escala in escalas_q4:
        valor = q4_tiempos[escala][idx]
        tiempos.append(valor if valor is not None else None)
    
    # Filtrar valores None para Neo4j
    if db == 'Neo4j':
        escalas_plot = escalas_q4[:3]
        tiempos_plot = tiempos[:3]
    else:
        escalas_plot = escalas_q4
        tiempos_plot = tiempos
    
    ax.plot(escalas_plot, tiempos_plot, label=db, 
            marker=markers[idx], color=color, linewidth=2, markersize=8)

ax.set_ylabel('Tiempo (ms)', fontsize=11)
ax.set_xlabel('Escala de usuarios', fontsize=11)
ax.set_title('Escalabilidad de Consulta Q4 (JOIN Simple)', fontsize=13, fontweight='bold')
ax.legend()
ax.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('escalabilidad_q4.png', dpi=300, bbox_inches='tight')
plt.show()