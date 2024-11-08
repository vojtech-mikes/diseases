from numpy import float64
import pandas as pd

weather = pd.read_excel("data/weather_jul_aug_sep_2023.xlsx")

smd = pd.read_excel(
    "data/smd_exp59-2023-24.xlsx", usecols="A:L", sheet_name="SMD_Path_Data"
)

planteye = pd.read_csv("data/planteye_exp59_smd.csv")

clean_planteye = planteye.drop(
    columns=[
        "g_alias",
        "Geno no",
        "Genotype",
        "treatment",
        "greenness bin0",
        "greenness bin1",
        "greenness bin2",
        "greenness bin3",
        "greenness bin4",
        "greenness bin5",
        "Height Max [mm]",
        "hue average [°]",
        "hue bin0",
        "hue bin1",
        "hue bin2",
        "hue bin3",
        "hue bin4",
        "hue bin5",
        "Leaf angle [°]",
        "Leaf area [mm²]",
        "Leaf area (projected) [mm²]",
        "Leaf inclination [mm²/mm²]",
        "NDVI bin0",
        "NDVI bin1",
        "NDVI bin2",
        "NDVI bin3",
        "NDVI bin4",
        "NDVI bin5",
        "NPCI bin0",
        "NPCI bin1",
        "NPCI bin2",
        "NPCI bin3",
        "NPCI bin4",
        "NPCI bin5",
        "PSRI bin0",
        "PSRI bin1",
        "PSRI bin2",
        "PSRI bin3",
        "PSRI bin4",
        "PSRI bin5",
    ]
)

clean_planteye = clean_planteye.map(lambda x: x.lower() if isinstance(x, str) else x)

clean_planteye.rename(
    columns={
        "Digital biomass [mm³]": "digital_biomass",
        "Leaf area index [mm²/mm²]": "leaf_area_index",
        "Light penetration depth [mm]": "light_pene_depth",
        "unit": "fullbarcode",
        "Height [mm]": "height",
    },
    inplace=True,
)

clean_planteye["fullbarcode"] = clean_planteye["fullbarcode"].apply(
    lambda x: x.replace(":", "-").replace("-0", "-")
)

for column in clean_planteye:
    clean_planteye.rename(
        columns={column: column.lower().replace(" ", "_")}, inplace=True
    )

clean_planteye["timestamp"] = pd.to_datetime(
    clean_planteye["timestamp"], format="%d-%m-%Y %H:%M"
).dt.normalize()

grouped_planteye = (
    clean_planteye.groupby(
        [
            "timestamp",
            "fullbarcode",
        ]
    )
    .mean(numeric_only=True)
    .reset_index()
)


smd["Date"] = pd.to_datetime(smd["Date"], format="%d.%m.%Y")

clean_smd = smd.drop(
    columns=[
        "unit column",
        "unit row",
        "Barcode",
        "Pot",
    ]
)

clean_smd = clean_smd.map(lambda x: x.lower() if isinstance(x, str) else x)

for column in clean_smd.columns:
    clean_smd.rename(columns={column: column.lower()}, inplace=True)

clean_smd.rename(
    columns={
        "position(barcode)": "fullbarcode",
        "percent_disease_index": "pdi",
        "date": "timestamp",
    },
    inplace=True,
)


grouped_smd = (
    clean_smd.groupby(["timestamp", "fullbarcode", "treatment"])
    .mean(numeric_only=True)
    .reset_index()
)

merged = pd.merge(grouped_smd, weather, on="timestamp", how="left")

for column in merged:
    exceptions = ["fullbarcode", "genotype", "timestamp", "treatment"]

    if column not in exceptions:
        merged[column] = merged[column].astype(float64)

merged = pd.merge(merged, grouped_planteye, on=["timestamp", "fullbarcode"], how="left")

merged.to_excel("results/dataset.xlsx", index=False)
merged.to_parquet("results/dataset.parquet", index=False)
