import pandas as pd

# from typing import List

# PMS_NUMERICAL_COLS = [
#     "NET_INTEREST_INCOME_YTD","REAL_INDOPEX_YTD","REAL_OPEX_YTD","FBI_YTD",
#     "RECOVERY_YTD","TOTAL_INTEREST_INCOME_YTD","TOTAL_INTEREST_EXPENSE_YTD","LOAN_VOLUME_POSITION",
#     "REGION_CODE","BRANCH_NAME","SEGMENT_DIV_OWNER","PRA_NPL", "NPL"
#     ]

def abs_handle_str(x):
    try:
        return abs(x)
    except:
        return 0


def generate_table_html(df):
    df_html = df.copy()
    df_html = convert_val_indo(df_html)
    df_html = handle_pct(df_html)
    return df_html.to_html(classes='data-table', index=False)

def handle_pct(df: pd.DataFrame) -> pd.DataFrame:
    df_pct = df.copy()
    PERCENTAGE_INDICATORS = {'persen', 'percent', 'pct', '%'}

    for col in df.columns:
        if any(indicator in col.lower() for indicator in PERCENTAGE_INDICATORS):
            df_pct[col] = df_pct[col].apply(lambda x: str(round(x, 1))+'%' if x is not None else x)

    return df_pct


def convert_val_indo(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    konversi = {
        1e12: "Triliun",
        1e9: "Miliar",
        1e6: "Juta",
        1e3: "Ribu"
    }

    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()

    for col in numeric_cols:
        for idx in df.index:
            value = df.loc[idx, col]

            if value == 0:
                continue

            abs_value = abs(value)

            for nilai, satuan in konversi.items():
                if abs_value >= nilai:
                    df.loc[idx, col] = f"{value/nilai:.2f} {satuan}"
                    break

    return df

def convert_nan_value_cols_to_0(df: pd.DataFrame) -> pd.DataFrame:

    return df
