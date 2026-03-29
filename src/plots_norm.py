import pandas as pd
import matplotlib.pyplot as plt

# -----------------------------
# LOAD DATA
# -----------------------------
df = pd.read_csv("benchmark_results.csv")

# Ensure proper types
df["buffer_size"] = df["buffer_size"].astype(int)
df["avg_time_ms"] = df["avg_time_ms"].astype(float)
df["hit_ratio"] = df["hit_ratio"].astype(float)

# Separate policies
lru = df[df["policy"] == "LRU"]
clock = df[df["policy"] == "Clock"]

# -----------------------------
# 1️⃣ Buffer Size vs Avg Time
# -----------------------------
plt.figure()
plt.plot(lru["buffer_size"], lru["avg_time_ms"], marker='o', label="LRU")
plt.plot(clock["buffer_size"], clock["avg_time_ms"], marker='o', label="Clock")
plt.xlabel("Buffer Pool Size")
plt.ylabel("Average Time (ms)")
plt.title("Buffer Size vs Average Fetch Time")
plt.legend()
plt.grid()
plt.savefig("buffer_vs_time.png")

# -----------------------------
# 2️⃣ Buffer Size vs Hit Ratio
# -----------------------------
plt.figure()
plt.plot(lru["buffer_size"], lru["hit_ratio"], marker='o', label="LRU")
plt.plot(clock["buffer_size"], clock["hit_ratio"], marker='o', label="Clock")
plt.xlabel("Buffer Pool Size")
plt.ylabel("Hit Ratio")
plt.title("Buffer Size vs Hit Ratio")
plt.legend()
plt.grid()
plt.savefig("buffer_vs_hitratio.png")

# -----------------------------
# 3️⃣ Policy vs Avg Time (Bar Plot)
# -----------------------------
plt.figure()
avg_time_policy = df.groupby("policy")["avg_time_ms"].mean()
avg_time_policy.plot(kind="bar")
plt.ylabel("Average Time (ms)")
plt.title("Policy vs Average Time")
plt.grid(axis='y')
plt.savefig("policy_vs_time.png")

# -----------------------------
# 4️⃣ Policy vs Hit Ratio (Bar Plot)
# -----------------------------
plt.figure()
hit_ratio_policy = df.groupby("policy")["hit_ratio"].mean()
hit_ratio_policy.plot(kind="bar")
plt.ylabel("Hit Ratio")
plt.title("Policy vs Hit Ratio")
plt.grid(axis='y')
plt.savefig("policy_vs_hitratio.png")

# -----------------------------
# 5️⃣ Buffer Size vs Evictions
# -----------------------------
plt.figure()
plt.plot(lru["buffer_size"], lru["evictions"], marker='o', label="LRU")
plt.plot(clock["buffer_size"], clock["evictions"], marker='o', label="Clock")
plt.xlabel("Buffer Pool Size")
plt.ylabel("Evictions")
plt.title("Buffer Size vs Evictions")
plt.legend()
plt.grid()
plt.savefig("buffer_vs_evictions.png")

# -----------------------------
# 6️⃣ Buffer Size vs Misses
# -----------------------------
plt.figure()
plt.plot(lru["buffer_size"], lru["misses"], marker='o', label="LRU")
plt.plot(clock["buffer_size"], clock["misses"], marker='o', label="Clock")
plt.xlabel("Buffer Pool Size")
plt.ylabel("Misses")
plt.title("Buffer Size vs Misses")
plt.legend()
plt.grid()
plt.savefig("buffer_vs_misses.png")

print("✅ All plots generated and saved!")