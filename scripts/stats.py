import pandas as pd
import seaborn as sb
import matplotlib.pyplot as plt


def doit(df1: pd.DataFrame, df2: pd.DataFrame, barcode: str):
    corr1 = df1.merge(df2, on=["fullbarcode", "timestamp"])

    corr1 = corr1.drop(columns=["timestamp"])

    tocorr = corr1.query("fullbarcode == @barcode")

    return tocorr.drop(columns=["fullbarcode"])


control = pd.read_parquet("results/control_final2.parquet")

measurement = pd.read_parquet("results/measurement_final2.parquet")

state = pd.read_parquet("results/state_final2.parquet")

corr1 = doit(measurement, state, "204-1-1")

result = corr1.corr()

sb.heatmap(result, fmt=".2f", annot=True, square=True)

plt.show()
