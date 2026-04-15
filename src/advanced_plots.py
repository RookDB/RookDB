import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# -----------------------------
# LOAD DATA
# -----------------------------
df = pd.read_csv("benchmark_results.csv")

df["buffer_size"] = df["buffer_size"].astype(int)
df["avg_time_ms"] = df["avg_time_ms"].astype(float)
df["hit_ratio"] = df["hit_ratio"].astype(float)

# ✅ ADD DERIVED COLUMNS FIRST
df["throughput"] = 1000 / df["avg_time_ms"]
df["miss_penalty"] = df["avg_time_ms"] / (1 - df["hit_ratio"] + 1e-6)
df["eviction_rate"] = df["evictions"] / (df["hits"] + df["misses"])

# THEN filter
lru = df[df["policy"] == "LRU"]
clock = df[df["policy"] == "Clock"]

# -----------------------------
# 1️⃣ Log Scale: Buffer vs Time
# -----------------------------
plt.figure()
plt.xscale("log")
plt.plot(lru["buffer_size"], lru["avg_time_ms"], marker='o', label="LRU")
plt.plot(clock["buffer_size"], clock["avg_time_ms"], marker='o', label="Clock")
plt.xlabel("Buffer Size (log scale)")
plt.ylabel("Avg Time (ms)")
plt.title("Buffer Size vs Time (Log Scale)")
plt.legend()
plt.grid()
plt.savefig("log_buffer_vs_time.png")

# -----------------------------
# 2️⃣ Throughput (ops/sec)
# -----------------------------
df["throughput"] = 1000 / df["avg_time_ms"]  # ops per second

plt.figure()
plt.plot(lru["buffer_size"], lru["throughput"], marker='o', label="LRU")
plt.plot(clock["buffer_size"], clock["throughput"], marker='o', label="Clock")
plt.xlabel("Buffer Size")
plt.ylabel("Throughput (ops/sec)")
plt.title("Buffer Size vs Throughput")
plt.legend()
plt.grid()
plt.savefig("throughput.png")

# -----------------------------
# 3️⃣ Miss Penalty Approximation
# -----------------------------
df["miss_penalty"] = df["avg_time_ms"] / (1 - df["hit_ratio"] + 1e-6)

plt.figure()
plt.plot(lru["buffer_size"], lru["miss_penalty"], marker='o', label="LRU")
plt.plot(clock["buffer_size"], clock["miss_penalty"], marker='o', label="Clock")
plt.xlabel("Buffer Size")
plt.ylabel("Miss Penalty (ms)")
plt.title("Estimated Miss Penalty")
plt.legend()
plt.grid()
plt.savefig("miss_penalty.png")

# -----------------------------
# 4️⃣ Hit vs Miss (Stacked Bar)
# -----------------------------
x = np.arange(len(df["buffer_size"].unique()))
width = 0.35

plt.figure()

plt.bar(x - width/2, lru["hit_ratio"], width, label="LRU Hits")
plt.bar(x - width/2, 1 - lru["hit_ratio"], width, bottom=lru["hit_ratio"], label="LRU Misses")

plt.bar(x + width/2, clock["hit_ratio"], width, label="Clock Hits")
plt.bar(x + width/2, 1 - clock["hit_ratio"], width, bottom=clock["hit_ratio"], label="Clock Misses")

plt.xticks(x, lru["buffer_size"])
plt.xlabel("Buffer Size")
plt.ylabel("Ratio")
plt.title("Hit vs Miss Distribution")
plt.legend()
plt.savefig("hit_vs_miss_stacked.png")

# -----------------------------
# 5️⃣ Eviction Pressure
# -----------------------------
df["eviction_rate"] = df["evictions"] / (df["hits"] + df["misses"])

plt.figure()
plt.plot(lru["buffer_size"], lru["eviction_rate"], marker='o', label="LRU")
plt.plot(clock["buffer_size"], clock["eviction_rate"], marker='o', label="Clock")
plt.xlabel("Buffer Size")
plt.ylabel("Eviction Rate")
plt.title("Eviction Pressure vs Buffer Size")
plt.legend()
plt.grid()
plt.savefig("eviction_pressure.png")

print("✅ Advanced plots generated!")