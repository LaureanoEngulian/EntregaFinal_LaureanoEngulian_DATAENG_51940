from typing import List

import requests
from commons import SparkETL

# Import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, concat


class AlphaVantageETL(SparkETL):
    def __init__(self):
        def __init__(self, job_name=None):
            super().__init__(job_name)

    def extract(self, symbol: str, api_key: str):
        """
        Get a "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key (Free API keys)
        Extract data from the AlphaVantage API for a specific symbol.
        """
        print(">>> [E] Extracting data from the API...")

        try:
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={symbol}&apikey={api_key}"
            response = requests.get(url)

            if response.status_code != 200:
                raise Exception(
                    f"API request failed with status code: {response.status_code}"
                )

            json_data = response.json().get("Monthly Time Series")

            data_rows = [
                Row(date=date, symbol=symbol, **values)
                for date, values in json_data.items()
            ]

            df = self.spark.createDataFrame(data_rows)
            df = (
                df.withColumnRenamed("1. open", "open")
                .withColumnRenamed("2. high", "high")
                .withColumnRenamed("3. low", "low")
                .withColumnRenamed("4. close", "close")
                .withColumnRenamed("5. volume", "volume")
            )

            return df
        
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None

    def combine_data(self, symbol_list: List[str], api_key: str):
        """
        Combine the extracted data for different symbols into a single DataFrame.
        """
        combined_data = None

        for symbol in symbol_list:
            data = self.extract(symbol, api_key)

            if data:
                combined_data = (
                    data if combined_data is None else combined_data.union(data)
                )

        return combined_data

    def transform(self, df_combined):
        """
        Perform transformations on the combined data.
        """
        df_combined = df_combined.withColumn(
            "avg", ((df_combined.open * df_combined.close) / 2).cast("decimal(18,4)")
        )
        df_combined = df_combined.withColumn(
            "pk", (concat(col("symbol"), col("week_from")))
        )

        df_combined.show()

        return df_combined
