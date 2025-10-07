import re
import sys
import pandas as pd
from tqdm import tqdm
from textblob import TextBlob
import string

def clean_text(s):
    printable = set(string.printable)
    return ''.join(ch for ch in s if ch in printable).strip()

def read_from_stdin():
    data = []
    print("Reading MapReduce outputs from stdin...")
    for line in tqdm(sys.stdin, desc="Processing lines"):
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

    if not data:
        print("No valid input received from stdin.")
        sys.exit(1)

    return pd.DataFrame(data, columns=["Key", "Value"])

def sentiment_analysis(df):
    results = []
    for word in tqdm(df["Key"], desc="Analyzing sentiment"):
        blob = TextBlob(word)
        polarity = blob.sentiment.polarity
        if polarity > 0.05:
            sentiment = "Positive"
        elif polarity < -0.05:
            sentiment = "Negative"
        else:
            sentiment = "Neutral"
        results.append((word, polarity, sentiment))
    sent_df = pd.DataFrame(results, columns=["Key", "Polarity", "Sentiment"])
    return df.merge(sent_df, on="Key", how="left")

if __name__ == "__main__":
    df = read_from_stdin()

    df = sentiment_analysis(df)

    print("\n=== Sentiment Summary ===")
    print(df["Sentiment"].value_counts())

    print("\nTop 10 Positive Words:")
    print(df[df["Sentiment"] == "Positive"].sort_values("Polarity", ascending=False).head(10)[["Key", "Polarity"]])

    print("\nTop 10 Negative Words:")
    print(df[df["Sentiment"] == "Negative"].sort_values("Polarity").head(10)[["Key", "Polarity"]])

    output_csv = "sentiment_analysis_output.csv"
    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"\nSentiment analysis results saved to: {output_csv}")
