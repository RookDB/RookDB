#!/usr/bin/env python3

"""Generate a large CSV to trigger external sort in RookDB.

Usage:
  python3 examples/generate_sorting_demo_large_csv.py
"""

from pathlib import Path


def main() -> None:
    output = Path("examples/sorting_demo_large.csv")
    row_count = 16000

    cities = [
        "Delhi",
        "Pune",
        "Mumbai",
        "Jaipur",
        "Noida",
        "Indore",
        "Bhopal",
        "Nagpur",
    ]
    depts = ["CSE", "EEE", "ME", "CE"]

    with output.open("w", encoding="ascii", newline="") as f:
        f.write("id,name,city,dept,score\n")
        for i in range(1, row_count + 1):
            unsorted_id = ((row_count - i + 1) * 37) % 20000
            name = f"S{i:05d}"[:10]
            city = cities[i % len(cities)]
            dept = depts[i % len(depts)]
            score = 50 + (i * 13) % 51
            f.write(f"{unsorted_id},{name},{city},{dept},{score}\n")

    print(f"Wrote {row_count} rows to {output}")


if __name__ == "__main__":
    main()
