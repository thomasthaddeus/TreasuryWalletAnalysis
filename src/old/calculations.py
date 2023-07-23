"""
_summary_

_extended_summary_

Returns:
    _type_: _description_
"""


# Importing important packages
import pandas as pd
import numpy as np

pd.set_option("display.float_format", lambda x: f"{x:.3f}")

data = pd.read_csv("chain_info.csv")
for index, row in data.iterrows():
    blockchain = row["blockchain"]

    data = pd.read_csv(f"data/{blockchain}.csv")

    data = data.drop(data.columns[0], axis=1)
    data["time"] = pd.to_datetime(data["time"]).dt.strftime("%Y-%m-%d")
    data = data.sort_values(by="time").reset_index()
    data = data.drop(data.columns[0], axis=1)
    data["calc_value"] = (
        data["calc_value"]
        .astype(float)
        .mul(np.where(data["category"] == "from", -1, 1))
    )
    data["price_usd"] = pd.Series(dtype="float")
    data = data.drop(["from"], axis=1)
    data = data.drop(["to"], axis=1)

    df = data[["time", "contract_address", "ticker", "token", "calc_value"]]

    # Create an empty dictionary to store the contract address as the key and name/ticker as the values
    contract_dict = {}

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        contract_address = row["contract_address"]
        token = row["token"]
        ticker = row["ticker"]

        # Check if the contract address is already present in the dictionary
        if contract_address not in contract_dict:
            # Add the contract address as the key and name/ticker as the values to the dictionary
            contract_dict[contract_address] = [token, ticker]

    # Convert the 'time' column to datetime format
    df["time"] = pd.to_datetime(df["time"])

    # Extract the month and year from the 'time' column
    df["month"] = df["time"].dt.to_period("M")

    # Group the data by 'contract_address' and 'month' and calculate the cumulative sum of 'calc_value'
    df_grouped = (
        df.groupby(["contract_address", "month"])["calc_value"]
        .sum()
        .groupby("contract_address")
        .cumsum()
    )

    # Reset the index to make it a regular column
    df_grouped = df_grouped.reset_index()

    # Convert the 'time' column to datetime format
    df["time"] = pd.to_datetime(df["time"])

    # Extract the month and year from the 'time' column
    df["month"] = df["time"].dt.to_period("M")

    # Group the data by 'contract_address' and 'month' and calculate the cumulative sum of 'calc_value'
    df_grouped = (
        df.groupby(["contract_address", "month"])["calc_value"]
        .sum()
        .groupby("contract_address")
        .cumsum()
    )

    # Reset the index to make it a regular column
    df_grouped = df_grouped.reset_index()

    # Pivot the DataFrame to reformat it as a table
    df_pivot = df_grouped.pivot(
        index="month", columns="contract_address", values="calc_value"
    )

    # Create a new index including all months from the earliest to the current month
    current_month = pd.to_datetime("today").to_period("M")
    full_index = pd.period_range(
        start=df_pivot.index.min(), end=current_month, freq="M"
    )

    # Reindex the df to include all months and fill missing values with NaN
    df_pivot = df_pivot.reindex(full_index)

    # Forward fill the missing values up to the current month
    df_pivot = df_pivot.fillna(method="ffill")

    # Fill remaining NaN values with 0
    df_pivot = df_pivot.fillna(value=0)

    # Create a new DataFrame with two additional rows for headers
    new_df = pd.DataFrame(columns=df_pivot.columns, index=range(2)).fillna("")

    # Populate the new DataFrame with dictionary values as headers
    for col, header in contract_dict.items():
        new_df[col] = header

    # Concatenate the new DataFrame with the original DataFrame
    df_with_headers = pd.concat([new_df, df_pivot])

    def promote_rows_as_headers(df):
        new_header = df.iloc[0:2]  # Get the first two rows as the new headers
        df = df[2:]  # Remove the first two rows from the DataFrame
        new_columns = pd.MultiIndex.from_arrays(
            [df.columns, *new_header.values]
        )  # Create a MultiIndex for the new headers
        df.columns = new_columns  # Set the new headers as column names
        # df.reset_index(drop=True, inplace=True)  # Reset the index

        return df

    new_df = promote_rows_as_headers(df_with_headers)

    # Write to CSV
    new_df.to_csv(f"data/{blockchain}_balance.csv")

excel_file = pd.ExcelFile("data/monthly_prices_full.xlsx")
sheet_names = excel_file.sheet_names

df1 = pd.DataFrame()
for sheet_name in sheet_names:
    monthly = pd.read_excel(excel_file, sheet_name)
    del monthly[monthly.columns[0]]
    cols = list(monthly.columns)  # Creating a list with the headers
    cols[0] = "date"  # Changing the first header value to "date"
    monthly.columns = cols  # Changing the actual column headers
    monthly = monthly.melt(
        id_vars=["date"],
        value_vars=monthly.columns[1:],
        var_name="contract_address",
        value_name="price",
    )
    monthly["date"] = pd.to_datetime(monthly["date"]).dt.to_period("m")

    chain = pd.read_csv(f"data/{sheet_name}_balance.csv", skiprows=[1, 2])
    cols = list(chain.columns)
    cols[0] = "date"
    chain.columns = cols
    chain = chain.melt(
        id_vars=["date"],
        value_vars=chain.columns[1:],
        var_name="contract_address",
        value_name="amount",
    )
    chain["date"] = pd.to_datetime(chain["date"]).dt.to_period("m")

    merged_dataframe = pd.merge(
        monthly, chain, how="inner", on=["date", "contract_address"]
    )
    merged_dataframe["value_usd"] = (
        merged_dataframe["price"] * merged_dataframe["amount"]
    )

    merged_dataframe = merged_dataframe.pivot(index="date", columns="contract_address")[
        "value_usd"
    ]

    row_sums = merged_dataframe.sum(axis=1).rename("usd_amount")
    row_sums = row_sums.reset_index()

    row_sums.to_csv(f"data/{sheet_name}_summed.csv")

    df1 = pd.concat([df1, row_sums], axis=0, ignore_index=True)

summed = df1.groupby("date").sum()
summed.to_csv("summed.csv")
