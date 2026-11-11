import os
import pandas as pd
from sqlalchemy import create_engine

DB_USER = "root"
DB_PASSWORD = "19992003"
DB_HOST = "host.docker.internal"
DB_PORT = "3306"
DB_NAME = "cde_assignment"

def get_db_engine():
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def transform_complaints_data():
    engine = get_db_engine()
    df = pd.read_sql("SELECT * FROM raw_consumer_complaints", con=engine)

    df["date_received"] = pd.to_datetime(df["date_received"])
    df["Month Year"] = df["date_received"].dt.to_period("M").dt.to_timestamp("M").dt.strftime("%Y-%m-%d")

    dim_cols = [
        "product",
        "sub_product",
        "issue",
        "sub_issue",
        "submitted_via",
        "company",
        "state",
        "timely",
        "company_response",
        "Month Year"
    ]

    for col in dim_cols:
        if col in df.columns:
            df[col] = df[col].fillna("N/A")

    df_transformed = (
        df.groupby(dim_cols, dropna=False)["complaint_id"]
        .nunique()
        .reset_index(name="Count of complaint_id")
    )

    output_path = os.path.join(os.path.dirname(__file__), "..", "DATA/consumer_complaints_transformed.csv")
    output_path = os.path.abspath(output_path)
    
    df_transformed.to_csv(output_path, index=False)
    print(f"Transformation complete. Saved {len(df_transformed)} rows to {output_path}")
    return output_path

if __name__ == "__main__":
    transform_complaints_data()
