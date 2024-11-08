import pandas as pd


def parse_planteye() -> pd.DataFrame:
    planteye = pd.read_csv("data/planteye_exp59_smd.csv")

    planteye = planteye.drop(
        columns=[
            "g_alias",
            "genotype",
            "Genotype",
            "treatment",
            "Geno no",
            "Rep",
            "greenness bin0",
            "greenness bin1",
            "greenness bin2",
            "greenness bin3",
            "greenness bin4",
            "greenness bin5",
            "hue average [°]",
            "hue bin0",
            "hue bin1",
            "hue bin2",
            "hue bin3",
            "hue bin4",
            "hue bin5",
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

    planteye = planteye.rename(
        columns={
            "unit": "barcode",
            "Digital biomass [mm³]": "biomass",
            "greenness average": "greenness_avg",
            "Height [mm]": "height",
            "Height Max [mm]": "height_max",
            "Leaf angle [°]": "leaf_angle",
            "Leaf area [mm²]": "leaf_area",
            "Leaf area index [mm²/mm²]": "lai",
            "Leaf area (projected) [mm²]": "leaf_area_proj",
            "Leaf inclination [mm²/mm²]": "leaf_inclination",
            "Light penetration depth [mm]": "light_pen_depth",
            "NDVI average": "ndvi_avg",
            "NPCI average": "npci_avg",
            "PSRI average": "psri_avg",
        }
    )

    planteye.columns = [col.lower() for col in planteye.columns]

    planteye["timestamp"] = pd.to_datetime(
        planteye["timestamp"], format="%d-%m-%Y %H:%M"
    ).dt.normalize()

    # Very  dirty but I know the data, so i can stay like this
    for col in planteye.columns:
        if planteye[col].dtype == "object":
            try:
                planteye[col] = pd.to_numeric(planteye[col])
            except ValueError:
                continue

    planteye["barcode"] = planteye["barcode"].apply(
        lambda x: x.replace(":", "-").replace("-0", "-")
    )

    planteye_grouped = planteye.groupby(["barcode", "timestamp"], as_index=False).mean()

    planteye_grouped = planteye_grouped.reset_index(drop=True)

    return planteye_grouped
