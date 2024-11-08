import pandas as pd


def parse_planteye() -> pd.DataFrame:
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

    clean_planteye = clean_planteye.map(
        lambda x: x.lower() if isinstance(x, str) else x
    )

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

    return grouped_planteye
