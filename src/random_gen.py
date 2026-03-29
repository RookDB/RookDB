import random

# Configuration
NUM_COUNT = 10000
LOW = 1
HIGH = 1000
OUTPUT_FILE = "random_numbers.txt"

# Generate numbers
numbers = [str(random.randint(LOW, HIGH)) for _ in range(NUM_COUNT)]

# Write to file (one number per line)
with open(OUTPUT_FILE, "w") as f:
    f.write("\n".join(numbers))

print(f"✅ Generated {NUM_COUNT} random numbers in '{OUTPUT_FILE}'")