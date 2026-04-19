import csv
import random
import string

def generate_random_name(min_len=5, max_len=10):
    """Generates a random string of letters."""
    length = random.randint(min_len, max_len)
    return ''.join(random.choices(string.ascii_letters, k=length))

def create_csv(filename, num_rows=100):
    """Generates the data and saves it to a CSV file."""
    header = ['id', 'name']
    
    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            
            for i in range(1, num_rows + 1):
                writer.writerow([i, generate_random_name()])
                
        print(f"Success! {num_rows} rows saved to {filename}")
    except IOError as e:
        print(f"Error writing to file: {e}")

if __name__ == "__main__":
    create_csv('op.csv', num_rows=5000)