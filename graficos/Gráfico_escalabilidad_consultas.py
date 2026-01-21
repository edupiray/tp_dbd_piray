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