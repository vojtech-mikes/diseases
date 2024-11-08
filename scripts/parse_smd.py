from typing import Callable, List
import pandas as pd
import numpy as np

pd.options.mode.copy_on_write = True


def filter_out(func: Callable, expr: List[str]) -> pd.DataFrame:
    result = pd.DataFrame()

    for i in expr:
        result = func(i)

    return result


def parse_smd() -> pd.DataFrame:
    smd = pd.read_excel(
        "data/smd_score_correct.xlsx",
        usecols="A:K",
        sheet_name="Veriticle Alignment",
        dtype={
            "Date": str,
        },
    )

    smd_date1 = smd.iloc[:720]

    smd_date2 = smd.iloc[720:]

    smd_date1["Date"] = pd.to_datetime(smd_date1["Date"], format="%d-%m-%Y")

    smd_date2["Date"] = pd.to_datetime(smd_date2["Date"], format="%Y-%d-%m %H:%M:%S")

    smd_fixed = pd.concat([smd_date1, smd_date2])

    with pd.option_context("future.no_silent_downcasting", True):
        smd_fixed = smd_fixed.replace(["no plant", "dead", "small plant"], np.nan)

    for c in ["Disease %", "Color", "Vigour"]:
        smd_fixed[c] = pd.to_numeric(smd_fixed[c])

    smd_fixed = smd_fixed.sort_values(
        by=["Position(barcode)", "Plant", "Date"], kind="stable"
    )

    smd_fixed = smd_fixed.drop(
        columns=[
            "Geno",
            "TYPE",
        ]
    )

    smd_fixed = smd_fixed.rename(
        columns={
            "Date": "timestamp",
            "Disease %": "Disease",
            "Position(barcode)": "barcode",
        }
    )

    smd_fixed.columns = [col.lower() for col in smd_fixed.columns]

    smd_fixed = smd_fixed.reset_index(drop=True)

    return smd_fixed
