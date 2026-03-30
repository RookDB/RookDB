#!/usr/bin/env python3
import matplotlib.pyplot as plt
import numpy as np

# Create figure with 2 subplots side by side
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# ===== UPDATE Chart =====
databases_update = ['RookDB', 'PostgreSQL']
update_times = [38721.181, 309.688]
colors_update = ['#e74c3c', '#27ae60']

bars1 = ax1.bar(databases_update, update_times, color=colors_update, width=0.5, edgecolor='black', linewidth=1.5)
ax1.set_ylabel('Time (ms)', fontsize=12, fontweight='bold')
ax1.set_title('UPDATE Average Latency (ms)', fontsize=13, fontweight='bold')
ax1.set_ylim(0, 40000)
ax1.grid(axis='y', alpha=0.3, linestyle='--')

# Add value labels on bars
for bar, val in zip(bars1, update_times):
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{val:,.1f} ms',
             ha='center', va='bottom', fontweight='bold', fontsize=11)

# Add note
ax1.text(0.5, 0.95, 'PostgreSQL is ~125x faster', 
         transform=ax1.transAxes, ha='center', va='top',
         fontsize=10, style='italic', color='#e74c3c',
         bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

# ===== DELETE & COMPACTION Chart =====
operations = ['DELETE\nRookDB', 'DELETE\nPostgreSQL', 'COMPACT\nRookDB', 'COMPACT\nPostgreSQL']
times = [26.007, 16.512, 12.362, 82.720]
colors_ops = ['#2196f3', '#27ae60', '#ff9800', '#e74c3c']

bars2 = ax2.bar(operations, times, color=colors_ops, width=0.6, edgecolor='black', linewidth=1.5)
ax2.set_ylabel('Time (ms)', fontsize=12, fontweight='bold')
ax2.set_title('DELETE and COMPACTION Average Latency (ms)', fontsize=13, fontweight='bold')
ax2.set_ylim(0, 100)
ax2.grid(axis='y', alpha=0.3, linestyle='--')

# Add value labels on bars
for bar, val in zip(bars2, times):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height,
             f'{val:.2f} ms',
             ha='center', va='bottom', fontweight='bold', fontsize=10)

# Add comparison note
ax2.text(0.5, 0.95, 'DELETE: PG ~1.58x faster | COMPACTION: RookDB ~6.69x faster', 
         transform=ax2.transAxes, ha='center', va='top',
         fontsize=9, style='italic', color='#333',
         bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))

plt.tight_layout()
plt.savefig('docs/static/assets/benchmark-charts.png', 
            dpi=300, bbox_inches='tight', facecolor='white')
print("✅ Chart saved to docs/static/assets/benchmark-charts.png")
plt.close()
