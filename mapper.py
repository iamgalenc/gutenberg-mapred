import sys
import re
stopwords = {"the", "and", "of", "in", "to", "a", "is", "it", "that", "for", "on", "with", "as", "was", "at", "by"}

for line in sys.stdin:
    line = line.strip().lower()
    line = re.sub(r'[^a-z\s]', ' ', line)
    words = line.split()
    words = re.findall(r'\b\w+\b', line)
    for word in words:
        if word not in stopwords and len(word) > 2:
            print(f"{word}\t1")