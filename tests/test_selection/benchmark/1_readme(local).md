Got you — here is a **clean copy-paste ready guide** (no confusion, no branching). Just follow exactly 👇

---

# 🚀 🔥 FULL SETUP (COPY–PASTE READY)

## ✅ Step 0: Go to your user (IMPORTANT)

Make sure you see:

```bash
surjit@surjit:~$
```

If not:

```bash
exit
```

---

# 🧱 Step 1: Fix PostgreSQL access (TEMPORARY)

```bash
sudo nano /etc/postgresql/*/main/pg_hba.conf
```

👉 Find this line:

```text
local   all             postgres                                md5
```

👉 Change to:

```text
local   all             postgres                                trust
```

---

## 💾 Save

```
CTRL + O → Enter  
CTRL + X
```

---

## 🔄 Restart

```bash
sudo service postgresql restart
```

---

# 🔑 Step 2: Login as postgres (no password now)

```bash
psql -U postgres
```

---

# 🔐 Step 3: Set ALL passwords (IMPORTANT)

```sql
ALTER USER postgres PASSWORD 'postgres';
ALTER USER bench_user PASSWORD 'bench_pass';
```

---

# 🧱 Step 4: Fix ownership + permissions

```sql
ALTER DATABASE bench_db OWNER TO bench_user;
GRANT ALL ON SCHEMA public TO bench_user;
```

---

## 🔄 Exit

```sql
\q
```

---

# 🔁 Step 5: Revert security (VERY IMPORTANT)

```bash
sudo nano /etc/postgresql/*/main/pg_hba.conf
```

👉 Change back:

```text
local   all             postgres                                md5
```

---

## 🔄 Restart again

```bash
sudo service postgresql restart
```

---

# 🐍 Step 6: Setup Python environment

```bash
cd ~/benchmark
python3 -m venv venv
source venv/bin/activate
pip install psycopg2-binary
```

---

# ▶️ Step 7: Run benchmark

```bash
python pg_benchmark.py
```

---

# 📁 Output file

```bash
~/benchmark/benchmark_output_pg.txt
```

---

# 🔑 PASSWORD SUMMARY (IMPORTANT)

| User            | Password              |
| --------------- | --------------------- |
| postgres        | `postgres`            |
| bench_user      | `bench_pass`          |
| system (surjit) | *your login password* |

---

# 🏁 FINAL RESULT

After this:

✔ No login issues
✔ No permission errors
✔ Benchmark runs fully
✔ Output file generated

---

# 🚀 After it runs

Send me:

* PostgreSQL output
* Your engine output

👉 I’ll give you:

* Final comparison
* Report conclusion (ready to submit)

---

If ANY step fails, just paste error — I’ll fix instantly.
