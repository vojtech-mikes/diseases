import pandas as pd


def parse_smd() -> pd.DataFrame:
    smd = pd.read_excel(
        "data/smd_exp59-2023-24.xlsx", usecols="A:L", sheet_name="SMD_Path_Data"
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

    return grouped_smd
