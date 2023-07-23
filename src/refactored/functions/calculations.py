"""calculations.py

This script contains a class DataAnalysis that provides methods for loading,
processing, cleaning, and analyzing token data. The class methods perform tasks
such as processing chain info, extracting required columns, grouping data,
calculating sums, and writing results to CSV files. The class is used to
generate detailed and summarized reports about tokens in different blockchains.
"""

import pandas as pd
import numpy as np


class DataAnalysis:
    """
    The DataAnalysis class contains methods for loading, processing,
    cleaning, and analyzing token data.

    Attributes:
        df1 (pd.DataFrame): A pandas DataFrame to store processed data.
    """

    def __init__(self):
        """
        Initialize a new instance of the DataAnalysis class.
        """
        pd.set_option("display.float_format", lambda x: f"{x:.3f}")
        self.df1 = pd.DataFrame()


    def process_chain_info(self):
        """
        Process the chain info data by iterating over each row, processing
        data and monthly prices for each blockchain.
        """
        data = pd.read_csv("chain_info.csv")
        for _, row in data.iterrows():
            blockchain = row["blockchain"]
            self.process_data(blockchain)
            self.process_monthly_prices(blockchain)

    def process_data(self, blockchain):
        """
        Process the data of a specific blockchain.

        Args:
            blockchain (str): The name of the blockchain.
        """
        data = self.load_data(blockchain)
        data = self.clean_data(data)
        df = self.get_required_columns(data)
        contract_dict = self.get_contract_dict(df)
        df_grouped = self.get_grouped_data(df)
        df_pivot = self.get_pivot_data(df_grouped)
        df_with_headers = self.add_headers(df_pivot, contract_dict)
        new_df = self.promote_rows_as_headers(df_with_headers)
        self.write_to_csv(new_df, blockchain)

    def load_data(self, blockchain):
        """
        Load the data of a specific blockchain from a CSV file.

        Args:
            blockchain (str): The name of the blockchain.

        Returns:
            DataFrame: The loaded data.
        """
        data = pd.read_csv(f"data/{blockchain}.csv")
        return data

    def clean_data(self, data):
        """
        Clean the data by reformatting time, sorting values, resetting index,
        calculating values, and dropping unnecessary columns.

        Args:
            data (DataFrame): The data to clean.

        Returns:
            DataFrame: The cleaned data.
        """
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
        return data

    def get_required_columns(self, data):
        """
        Extract the required columns from the data.

        Args:
            data (DataFrame): The data.

        Returns:
            DataFrame: The DataFrame with only the required columns.
        """
        df = data[["time", "contract_address", "ticker", "token", "calc_value"]]
        return df

    def get_contract_dict(self, df):
        """
        Get the contract dictionary from the DataFrame.

        Args:
            df (DataFrame): The DataFrame.

        Returns:
            dict: The contract dictionary.
        """
        contract_dict = {}
        for _, row in df.iterrows():
            contract_address = row["contract_address"]
            token = row["token"]
            ticker = row["ticker"]
            if contract_address not in contract_dict:
                contract_dict[contract_address] = [token, ticker]
        return contract_dict

    def get_grouped_data(self, df):
        """
        Get the grouped data by aggregating calc_value by contract_address and
        month.

        Args:
            df (DataFrame): The DataFrame.

        Returns:
            DataFrame: The grouped data.
        """
        df["time"] = pd.to_datetime(df["time"])
        df["month"] = df["time"].dt.to_period("M")
        df_grouped = (
            df.groupby(["contract_address", "month"])["calc_value"]
            .sum()
            .groupby("contract_address")
            .cumsum()
        )
        df_grouped = df_grouped.reset_index()
        return df_grouped

    def get_pivot_data(self, df_grouped):
        """
        Get the pivot data from the grouped data.

        Args:
            df_grouped (DataFrame): The grouped data.

        Returns:
            DataFrame: The pivot data.
        """
        df_pivot = df_grouped.pivot(
            index="month", columns="contract_address", values="calc_value"
        )
        current_month = pd.to_datetime("today").to_period("M")
        full_index = pd.period_range(
            start=df_pivot.index.min(), end=current_month, freq="M"
        )
        df_pivot = df_pivot.reindex(full_index)
        df_pivot = df_pivot.fillna(method="ffill")
        df_pivot = df_pivot.fillna(value=0)
        return df_pivot

    def add_headers(self, df_pivot, contract_dict):
        """
        Add headers to the pivot DataFrame.

        Args:
            df_pivot (DataFrame): The pivot DataFrame.
            contract_dict (dict): The contract dictionary.

        Returns:
            DataFrame: The DataFrame with added headers.
        """
        new_df = pd.DataFrame(
            columns=df_pivot.columns,
            index=range(2)
        ).fillna("")
        for col, header in contract_dict.items():
            new_df[col] = header
        df_with_headers = pd.concat([new_df, df_pivot])
        return df_with_headers

    def promote_rows_as_headers(self, df_with_headers):
        """
        Promote rows as headers for the DataFrame.

        Args:
            df_with_headers (DataFrame): The DataFrame with headers.

        Returns:
            DataFrame: The DataFrame with promoted headers.
        """
        new_header = df_with_headers.iloc[0:2]
        df = df_with_headers[2:]
        new_columns = pd.MultiIndex.from_arrays(
            [df.columns, *new_header.values]
        )
        df.columns = new_columns
        return df

    def write_to_csv(self, new_df, blockchain):
        """
        Write a DataFrame to a CSV file.

        Args:
            new_df (DataFrame): The DataFrame to write.
            blockchain (str): The name of the blockchain.
        """
        new_df.to_csv(f"data/{blockchain}_balance.csv")

    def process_monthly_prices(self, blockchain):
        """
        Process the monthly prices of a specific blockchain.

        Args:
            blockchain (str): The name of the blockchain.
        """
        excel_file = self.load_excel_file()
        for sheet_name in excel_file.sheet_names:
            monthly = self.load_monthly_data(excel_file, sheet_name)
            chain = self.load_chain_data(sheet_name)
            merged_dataframe = self.merge_data(monthly, chain)
            row_sums = self.calculate_sums(merged_dataframe)
            self.write_sums_to_csv(row_sums, sheet_name)

    def load_excel_file(self):
        """
        Load the Excel file with monthly prices.

        Returns:
            ExcelFile: The loaded Excel file.
        """
        excel_file = pd.ExcelFile("data/monthly_prices_full.xlsx")
        return excel_file

    def load_monthly_data(self, excel_file, sheet_name):
        """
        Load the monthly data from an Excel file.

        Args:
            excel_file (ExcelFile): The Excel file.
            sheet_name (str): The name of the sheet.

        Returns:
            DataFrame: The monthly data.
        """
        monthly = pd.read_excel(excel_file, sheet_name)
        del monthly[monthly.columns[0]]
        cols = list(monthly.columns)
        cols[0] = "date"
        monthly.columns = cols
        monthly = monthly.melt(
            id_vars=["date"],
            value_vars=monthly.columns[1:],
            var_name="contract_address",
            value_name="price",
        )
        monthly["date"] = pd.to_datetime(monthly["date"]).dt.to_period("m")
        return monthly

    def load_chain_data(self, sheet_name):
        """
        Load the chain data from a CSV file.

        Args:
            sheet_name (str): The name of the sheet.

        Returns:
            DataFrame: The chain data.
        """

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
        return chain

    def merge_data(self, monthly, chain):
        """
        Merge the monthly and chain data.

        Args:
            monthly (DataFrame): The monthly data.
            chain (DataFrame): The chain data.

        Returns:
            DataFrame: The merged data.
        """
        merged_dataframe = pd.merge(
            monthly, chain, how="inner", on=["date", "contract_address"]
        )
        merged_dataframe["value_usd"] = (
            merged_dataframe["price"] * merged_dataframe["amount"]
        )
        merged_dataframe = merged_dataframe.pivot(index="date", columns="contract_address")[
            "value_usd"
        ]
        return merged_dataframe

    def calculate_sums(self, merged_dataframe):
        """
        Calculate the sums of the merged data.

        Args:
            merged_dataframe (DataFrame): The merged data.

        Returns:
            Series: The sums of the merged data.
        """
        row_sums = merged_dataframe.sum(axis=1).rename("usd_amount")
        row_sums = row_sums.reset_index()
        return row_sums

    def write_sums_to_csv(self, row_sums, sheet_name):
        """
        Write the sums to a CSV file.

        Args:
            row_sums (Series): The sums.
            sheet_name (str): The name of the sheet.
        """
        row_sums.to_csv(f"data/{sheet_name}_summed.csv")

    def run(self):
        """
        Run the data analysis by processing chain info and writing the
        summed data to a CSV file.
        """
        self.process_chain_info()
        summed = self.df1.groupby("date").sum()
        summed.to_csv("summed.csv")
