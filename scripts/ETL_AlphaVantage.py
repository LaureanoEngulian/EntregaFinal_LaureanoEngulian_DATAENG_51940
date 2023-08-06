from os import environ as env
import requests
# Import SparkSession
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import (col, concat, expr, lag, lit, max, round,
                                   to_date, when)
from pyspark.sql.window import Window

from commons import SparkETL

class AlphaVantageETL(SparkETL):
    def __init__(self):
         def __init__(self, job_name=None):
            super().__init__(job_name)

    def extract(self, symbol):
        """
        Get a "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key (Free API keys)
        Extract data from the AlphaVantage API for a specific symbol.
        """
        print(">>> [E] Extracting data from the API...")

        try:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol={symbol}&apikey={env["API_KEY"]}'
            response = requests.get(url)
            
            if response.status_code != 200:
                raise Exception(f"API request failed with status code: {response.status_code}")
            
            json_data = response.json().get("Monthly Time Series")

            data_rows = [
                Row(date=date, symbol=symbol, **values)
                for date, values in json_data.items()
            ]

            df = self.spark.createDataFrame(data_rows)
            df = df.withColumnRenamed("1. open", "open") \
                .withColumnRenamed("2. high", "high") \
                .withColumnRenamed("3. low", "low") \
                .withColumnRenamed("4. close", "close") \
                .withColumnRenamed("5. volume", "volume")
            
            # df.show(truncate=True)
            return df
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None
       

    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        df_original.rename(
            columns={
                "index": "week_from",
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume",
            },
            inplace=True,
        )

        df_original["open"] = pd.to_numeric(df_original["open"])
        df_original["close"] = pd.to_numeric(df_original["close"])
        df_original["avg"] = (df_original["open"] + df_original["close"]) / 2
        df_original["pk"] = df_original["symbol"] + df_original["week_from"]

        df_original.reset_index(inplace=True)
        print(df_original.head())

        return df_original

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")
        print(datetime.now())

        url = URL.create(
            drivername="redshift+redshift_connector",  # indicate redshift_connector driver and dialect will be used
            host=env.get("HOST"),  # Amazon Redshift host
            port=int(env.get("PORT")),  # Amazon Redshift port
            database=env.get("DATABASE"),  # Amazon Redshift database
            username=env.get("USER"),  # Amazon Redshift username
            password=env.get("PASSWORD"),  # Amazon Redshift password
        )

        engine = sa.create_engine(url)

        df_final.to_sql(name="stage", con=engine, if_exists="append", index=False)

        # Connect to Redshift using psycopg2
        conn = psycopg2.connect(
            host=env.get("HOST"),
            port=int(env.get("PORT")),
            database=env.get("DATABASE"),
            user=env.get("USER"),
            password=env.get("PASSWORD"),
        )

        cursor = conn.cursor()

        # https://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html
        sql_transaction = """ begin transaction;

                            
                            CREATE TABLE IF NOT EXISTS laureanoengulian_coderhouse.big_five_weekly(
                            "week_from" varchar(256) not null,
                            "open" float not null,
                            "high" varchar(256) not null,
                            "low" varchar(256) not null,
                            "close" float not null,
                            "volume" varchar(256) not null,
                            "symbol" varchar(256) not null,
                            "avg" float not null,
                            "pk" varchar(256));


                            delete from laureanoengulian_coderhouse.big_five_weekly using laureanoengulian_coderhouse.stage 
                            where big_five_weekly.pk = stage.pk;

                            insert into laureanoengulian_coderhouse.big_five_weekly
                            select CAST("week_from" as varchar(256)) as "week_from", 
                                   CAST("open" as float) as "open",
                                   CAST("high" as varchar(256)) as "high",
                                   CAST("low" as varchar(256)) as "low",
                                   CAST("close" as float) as "close",
                                   CAST("volume" as varchar(256)) as "volume",
                                   CAST("symbol" as varchar(256)) as "volume",
                                   CAST("avg" as float) as "avg",
                                   CAST("pk" as varchar(256)) as "pk"  
                            from laureanoengulian_coderhouse.stage;

                            end transaction;
                        """

        cursor.execute(sql_transaction)
        conn.commit()

        drop_tmp = """drop table if exists laureanoengulian_coderhouse.stage;"""

        cursor.execute(drop_tmp)
        conn.commit()

        # Chequeo de valores únicos
        cursor = conn.cursor()
        cursor.execute(
            f"""
        SELECT
        count(pk), count(distinct pk)
        FROM
        laureanoengulian_coderhouse.big_five_weekly;
        """
        )
        # resultado = cursor.fetchall()
        print(", ".join(map(lambda x: str(x), cursor.fetchall())))
        cursor.close()

        print(">>> [L] Datos cargados exitosamente")

    def run(self):
        raw_data = self.extract()
        # self.transform(raw_data)
        clean_data = self.transform(raw_data)
        self.load(clean_data)


if __name__ == "__main__":
    print("Running script")
    etl = AlphaVantageETL()
    etl.run()
