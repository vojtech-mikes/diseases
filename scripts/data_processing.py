import pandas as pd

from parse_smd import parse_smd
from parse_planteye import parse_planteye

print("Starting processing...")

weather = pd.read_excel("data/weather_jul_aug_sep_2023.xlsx")

smd = parse_smd()

planteye = parse_planteye()

smd_weather = pd.merge(smd, weather, on="timestamp", how="left")

smd_weather_planteye = pd.merge(
    smd_weather, planteye, on=["timestamp", "barcode"], how="left"
)

print("Processing done...")

print("Saving dataset to results")

smd_weather_planteye.to_excel("results/final_dataset.xlsx", index=False)

smd_weather_planteye.to_parquet("results/final_dataset.parquet", index=False)
