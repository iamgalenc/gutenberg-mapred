import re
import sys
import pandas as pd
from tqdm import tqdm
from pathlib import Path
import string
from io import StringIO
from tempfile import NamedTemporaryFile 

def clean_text(s):
    printable = set(string.printable)
    return ''.join(ch for ch in s if ch in printable).strip()

def read_mapreduce_output(file_path):
    data = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in tqdm(f, desc="Reading MapReduce outputs"):
            line = clean_text(line)
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) == 2:
                key, value = parts
            else:
                parts = re.split(r'\s+', line)
                if len(parts) < 2:
                    continue
                key, value = " ".join(parts[:-1]), parts[-1]

            try:
                value = int(value)
                data.append((key, value))
            except ValueError:
                continue
    return pd.DataFrame(data, columns=["Key", "Value"])

def descriptive_statistics(df):
    stats = {
        "Total Unique Keys": df["Key"].nunique(),
        "Total Pairs": len(df),
        "Total Frequency": df["Value"].sum(),
        "Mean": df["Value"].mean(),
        "Median": df["Value"].median(),
        "Mode": df["Value"].mode().iloc[0] if not df["Value"].mode().empty else None,
        "Min": df["Value"].min(),
        "Max": df["Value"].max(),
        "Std": df["Value"].std()
    }
    return pd.Series(stats)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        input_text = sys.stdin.read()
        if not input_text.strip():
            print("No input provided.")
            sys.exit(1)
        with NamedTemporaryFile(delete=False, mode='w+', encoding='utf-8') as tmp:
            tmp.write(input_text)
            tmp.flush()
            file_path = Path(tmp.name)
    else:
        file_path = Path(sys.argv[1])
        
    df = read_mapreduce_output(file_path)

    if df.empty:
        print("No valid data found.")
    else:
        print("\n=== Descriptive Statistics ===")
        print(descriptive_statistics(df))
        print("\nTop 10 Most Frequent Keys:")
        print(df.sort_values("Value", ascending=False).head(10))
