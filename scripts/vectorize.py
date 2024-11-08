import pandas as pd
from sqlalchemy import create_engine

dataset = pd.read_parquet("results/dataset.parquet")

state = dataset[
    [
        "timestamp",
        "fullbarcode",
        "digital_biomass",
        "greenness_average",
        "leaf_area_index",
        "light_pene_depth",
        "height",
        "ndvi_average",
        "npci_average",
        "psri_average",
    ]
]

control = dataset[
    [
        "timestamp",
        "fullbarcode",
        "max_temp",
        "avg_temp",
        "min_temp",
        "max_humi",
        "avg_humi",
        "min_humi",
    ]
]

measurement = dataset[
    [
        "timestamp",
        "fullbarcode",
        "pdi",
    ]
]


barcodes = dataset[
    [
        "fullbarcode",
    ]
].drop_duplicates()

barcode_info = dataset[
    [
        "fullbarcode",
        "plants/pot",
        "treatment",
        "scale",
        "leaves_observed",
    ]
]


username = "root"
host = "localhost"
port = 3306
database = "disease_prediction2"

engine = create_engine(f"mysql+pymysql://{username}@{host}:{port}/{database}")

state.to_parquet("results/state_final2.parquet")
measurement.to_parquet("results/measurement_final2.parquet")
control.to_parquet("results/control_final2.parquet")

# barcodes.to_sql("barcodes", con=engine, if_exists="append", index=False)
#
# barcode_info.to_sql("barcode_info", con=engine, if_exists="append", index=False)
#
# state.to_sql("states", con=engine, if_exists="append", index=False)
#
# measurement.to_sql("measurements", con=engine, if_exists="append", index=False)
#
# control.to_sql("controls", con=engine, if_exists="append", index=False)
