from numpy import float64
import pandas as pd

from scripts.parse_planteye import parse_planteye
from scripts.parse_smd import parse_smd

weather = pd.read_excel("data/weather_jul_aug_sep_2023.xlsx")

grouped_planteye = parse_planteye()

grouped_smd = parse_smd()

merged = pd.merge(grouped_smd, weather, on="timestamp", how="left")

for column in merged:
    exceptions = ["fullbarcode", "genotype", "timestamp", "treatment"]

    if column not in exceptions:
        merged[column] = merged[column].astype(float64)

merged = pd.merge(merged, grouped_planteye, on=["timestamp", "fullbarcode"], how="left")

merged.to_excel("results/dataset.xlsx", index=False)
merged.to_parquet("results/dataset.parquet", index=False)
