# Este script está pensado para correr en Spark y hacer el proceso de ETL
from os import environ as env
import time
from datetime import datetime

import pandas as pd
import psycopg2
import requests
import sqlalchemy as sa
from sqlalchemy.engine.url import URL

class ETL_AlphaVantage:
    def __init__(self):
        print(datetime.now())

    def extract(self):
        """
        Extrae datos de la API AlphaVantage
        Replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key (Free API keys)
        Big Five Tech: Google, Amazon, Meta, Apple, and Microsoft (GAMAM)
        """
        print(">>> [E] Extrayendo datos de la API...")

        function = 'TIME_SERIES_WEEKLY'
        symbols = ['GOOG', 'AMZN', 'METV', 'AAPL', 'MSFT']
        api_key = 'WNHSBPLUX5B8HNMZ'

        df = pd.DataFrame()

        for symbol in symbols:
            url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={api_key}'
            r = requests.get(url)
            data = r.json()

            time.sleep(5)

            symbol_df = pd.DataFrame(data['Weekly Time Series'])
            symbol_df = symbol_df.T
            symbol_df.reset_index(inplace=True)

            symbol_df['symbol'] = data['Meta Data']['2. Symbol']

            df = pd.concat([df, symbol_df])

        return df

    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        df_original.rename(columns={'index': 'week_from', '1. open': 'open', '2. high': 'high', '3. low': 'low',
                                    '4. close': 'close', '5. volume': 'volume'}, inplace=True)
        
        df_original['open'] = pd.to_numeric(df_original['open'])
        df_original['close'] = pd.to_numeric(df_original['close'])
        df_original['avg'] = (df_original['open'] + df_original['close'])/2
        df_original['pk'] = df_original['symbol']+df_original['week_from']

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
        drivername='redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
        host=env.get('HOST'), # Amazon Redshift host
        port=int(env.get('PORT')), # Amazon Redshift port
        database=env.get('DATABASE'), # Amazon Redshift database
        username=env.get('USER'), # Amazon Redshift username
        password=env.get('PASSWORD') # Amazon Redshift password
        )

        engine = sa.create_engine(url)

        df_final.to_sql(name='stage',
                        con=engine,
                        if_exists='append',
                        index=False)

        # Connect to Redshift using psycopg2
        conn = psycopg2.connect(
            host=env.get('HOST'),
            port=int(env.get('PORT')),
            database=env.get('DATABASE'),
            user=env.get('USER'),
            password=env.get('PASSWORD')
        )

        cursor = conn.cursor()


        # https://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html
        sql_transaction = ''' begin transaction;

                            
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
                        '''

        cursor.execute(sql_transaction)
        conn.commit()

        drop_tmp = '''drop table if exists laureanoengulian_coderhouse.stage;'''

        cursor.execute(drop_tmp)
        conn.commit()

        # Chequeo de valores únicos
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT
        count(pk), count(distinct pk)
        FROM
        laureanoengulian_coderhouse.big_five_weekly;
        """)
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
    print("Corriendo script")
    etl = ETL_AlphaVantage()
    etl.run()
