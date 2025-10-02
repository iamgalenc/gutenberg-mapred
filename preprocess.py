import os
import re
from tqdm import tqdm

folder = r"gutenberg"
output_file = r"gutenberg_cleaned.txt"
txt_files = [f for f in os.listdir(folder) if f.endswith(".txt")]

def clean_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r'[^a-z\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.replace("_", " ")
    return text.strip()
    
def tokenizer(text: str):
    return re.findall(r'\b\w+\b', text)

with open(output_file, "w", encoding="utf-8") as outfile:
    for filename in tqdm(txt_files, desc="Menggabungkan file Gutenberg"):
        file_path = os.path.join(folder, filename)
        with open(file_path, "r", encoding="utf-8", errors="ignore") as infile:
            raw_text = infile.read()
            cleaned_text = clean_text(raw_text)
            tokens = tokenizer(cleaned_text)
            outfile.write(" ".join(tokens))
            outfile.write("\n")

print(f"Selesai. {len(txt_files)} file digabung ke: {output_file}")
