import pandas as pd
import re
import boto3
import io
import torch
from transformers import pipeline
from transformers.pipelines.pt_utils import KeyDataset
from tqdm.auto import tqdm

# ==========================================
# CONFIG
# ==========================================

S3_BUCKET = "steam-dataset-2025-bucket"

REVIEWS_INPUT_KEY = (
    "Steam/steam_dataset_2025_csv_package_v1/"
    "steam_dataset_2025_csv/reviews.csv"
)

OUTPUT_KEY = "silver/reviews/reviews_scored_final.csv"

MODEL_PATH = "./sentiment_model_local"  
# or "models/sentiment_model_local" if loaded from S3

BATCH_SIZE = 32

star_map = {
    1: "Worse",
    2: "Bad",
    3: "Neutral",
    4: "Good",
    5: "Best"
}

# ==========================================
# 1. READ REVIEWS FROM S3
# ==========================================

s3 = boto3.client("s3")

print("Downloading reviews.csv from S3...")
obj = s3.get_object(Bucket=S3_BUCKET, Key=REVIEWS_INPUT_KEY)
raw_data = obj["Body"].read().decode("utf-8", errors="replace")

print("Parsing reviews (handling multiline text)...")

cleaned_data = []
current_id = None
current_text = []

new_row_pattern = re.compile(r'^(\d+),(.*)')

for line in raw_data.splitlines():
    line = line.strip()
    match = new_row_pattern.match(line)

    if match:
        if current_id is not None:
            cleaned_data.append({
                "recommendationid": current_id,
                "review_text": " ".join(current_text)
            })

        current_id = match.group(1)
        current_text = [match.group(2)]

    else:
        if current_id is not None:
            current_text.append(line)

if current_id is not None:
    cleaned_data.append({
        "recommendationid": current_id,
        "review_text": " ".join(current_text)
    })

df_clean = pd.DataFrame(cleaned_data)

print(f"Parsed {len(df_clean)} reviews.")

# ==========================================
# 2. LOAD SENTIMENT MODEL
# ==========================================

device = 0 if torch.cuda.is_available() else -1
print(f"Using device: {'GPU' if device == 0 else 'CPU'}")

sentiment_pipeline = pipeline(
    task="sentiment-analysis",
    model=MODEL_PATH,
    tokenizer=MODEL_PATH,
    device=device,
    truncation=True,
    max_length=512,
    batch_size=BATCH_SIZE
)

# ==========================================
# 3. RUN INFERENCE
# ==========================================

results = []

data_stream = KeyDataset(
    df_clean.to_dict("records"),
    "review_text"
)

print("Running sentiment inference...")

for i, out in tqdm(enumerate(sentiment_pipeline(data_stream)), total=len(df_clean)):

    if isinstance(out, list):
        out = out[0]

    try:
        star = int(out["label"].split()[0])
        category = star_map.get(star, "Unknown")
    except Exception:
        star = -1
        category = "Error"

    results.append({
        "recommendationid": df_clean.iloc[i]["recommendationid"],
        "numeric_score": star,
        "category": category
    })

# ==========================================
# 4. WRITE RESULT BACK TO S3
# ==========================================

output_df = pd.DataFrame(results)

csv_buffer = io.StringIO()
output_df.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=S3_BUCKET,
    Key=OUTPUT_KEY,
    Body=csv_buffer.getvalue()
)

print(f"✅ Sentiment scores written to s3://{S3_BUCKET}/{OUTPUT_KEY}")
