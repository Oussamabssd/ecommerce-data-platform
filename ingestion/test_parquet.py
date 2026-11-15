from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "raw" / "postgres"


def validate_files():
    files = list(DATA_DIR.glob("*.parquet"))

    print(f"Found {len(files)} Parquet files")

    for file in files:
        dataframe = pd.read_parquet(file)

        print(f"\nFile: {file.name}")
        print(f"Rows: {len(dataframe)}")
        print(f"Columns: {list(dataframe.columns)}")
        print(dataframe.head(3))


if __name__ == "__main__":
    validate_files()
