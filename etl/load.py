import os
import pandas as pd
from sqlalchemy import create_engine
import gspread
from google.oauth2.service_account import Credentials

try:
    from etl.extract import extract_complaints_data
except ModuleNotFoundError:
    from extract import extract_complaints_data

DB_USER = "root"
DB_PASSWORD = "19992003"
DB_HOST = "host.docker.internal"
DB_PORT = "3306"
DB_NAME = "cde_assignment"

def get_db_engine():
    connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def dump_raw_data_to_mysql(df_raw=None):
    if df_raw is None:
        df_raw = extract_complaints_data()

    engine = get_db_engine()
    df_raw.to_sql(name="raw_consumer_complaints", con=engine, if_exists="replace", index=False)
    print(f"Successfully loaded {len(df_raw)} records into MySQL table 'raw_consumer_complaints'.")

def dump_to_google_sheet(csv_path=None, sheet_name="CFPB_Complaints_Transformed"):
    if csv_path is None:
        csv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "consumer_complaints_transformed.csv"))

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    df = pd.read_csv(csv_path)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    cred_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "credentials.json"))
    credentials = Credentials.from_service_account_file(cred_path, scopes=scopes)
    gc = gspread.authorize(credentials)

    spreadsheet = gc.open(sheet_name)
    worksheet = spreadsheet.sheet1

    worksheet.clear()
    data_to_upload = [df.columns.values.tolist()] + df.fillna("").values.tolist()
    worksheet.update(data_to_upload)
    
    print(f"Successfully dumped {len(df)} rows to Google Sheet: {spreadsheet.url}")
    return spreadsheet.url

if __name__ == "__main__":
    dump_to_google_sheet()
