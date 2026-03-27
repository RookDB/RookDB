#!/usr/bin/env python3
"""
Workload Generator for RookDB Benchmarking.
Generates command sequences for different scenarios:
- read_heavy (95% Reads, 5% Writes/Updates)
- write_heavy (20% Reads, 80% Writes)
- mixed (50% Reads, 50% Writes)

Output CSV format:
operation,key,customer_id,amount_cents

where operation is:
'I' (Insert)
'R' (Read)
'U' (Update)
"""

import argparse
import csv
import random
from pathlib import Path

def generate_workload(args):
    random.seed(args.seed)
    
    keys_in_db = set()
    next_insert_key = 1
    
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["operation", "key", "customer_id", "amount_cents"])
        
        # --- PHASE 1: LOAD (Always inserts to build initial state) ---
        print(f"Generating Load Phase ({args.load_size} rows)...")
        for _ in range(args.load_size):
            k = next_insert_key
            next_insert_key += 1
            customer_id = random.randint(1, 100000)
            amount_cents = random.randint(100, 50000)
            
            writer.writerow(["I", k, customer_id, amount_cents])
            keys_in_db.add(k)
        
        # --- PHASE 2: RUN (The actual scenario) ---
        print(f"Generating Workload Phase ({args.ops} operations) for scenario '{args.scenario}'...")
        
        if args.scenario == "read_heavy":
            read_prob = 0.95
            update_prob = 0.03
            insert_prob = 0.02
        elif args.scenario == "write_heavy":
            read_prob = 0.20
            update_prob = 0.40
            insert_prob = 0.40
        else: # mixed
            read_prob = 0.50
            update_prob = 0.25
            insert_prob = 0.25
            
        choices = ["R", "U", "I"]
        weights = [read_prob, update_prob, insert_prob]
        
        keys_list = list(keys_in_db)
        
        for i in range(args.ops):
            op = random.choices(choices, weights=weights, k=1)[0]
            
            if op == "R":
                if not keys_in_db: continue
                # Random read
                k = random.choice(keys_list)
                writer.writerow(["R", k, "", ""])
                
            elif op == "U":
                if not keys_in_db: continue
                k = random.choice(keys_list)
                customer_id = random.randint(1, 100000)
                amount_cents = random.randint(100, 50000)
                writer.writerow(["U", k, customer_id, amount_cents])
                
            elif op == "I":
                k = next_insert_key
                next_insert_key += 1
                customer_id = random.randint(1, 100000)
                amount_cents = random.randint(100, 50000)
                writer.writerow(["I", k, customer_id, amount_cents])
                
                keys_in_db.add(k)
                keys_list.append(k) # append to local array for fast access
                
            if (i+1) % 25000 == 0:
                print(f"  Generated {i+1} operations...")

    print(f"Done! Workload '{args.scenario}' saved to {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Generate RookDB benchmarking workloads")
    parser.add_argument("--scenario", choices=["read_heavy", "write_heavy", "mixed"], required=True, 
                        help="The type of workload scenario to generate")
    parser.add_argument("--load-size", type=int, default=10000, 
                        help="Number of initial rows to insert before workload starts")
    parser.add_argument("--ops", type=int, default=50000, 
                        help="Number of operations in the workload phase")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--output", type=str, required=True, help="Path to output CSV file")
    
    args = parser.parse_args()
    generate_workload(args)

if __name__ == "__main__":
    main()