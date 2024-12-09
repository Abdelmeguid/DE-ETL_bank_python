#import pkgs
import pandas as pd
import numpy as np
import requests
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup

def log_message(message):
    now=datetime.now()
    time_foramt_stamp=now.strftime("%Y-%m-%d-%H:%M:%S")
    print(f"{time_foramt_stamp}:{message}")

def extract(url,table_attribs):
    df=pd.DataFrame(columns=table_attribs)
    html_page=requests.get(url).text
    data=BeautifulSoup(html_page,"html.parser")
    tables=data.find_all("tbody")
    rows=tables[0].find_all("tr")
    for row in rows:
        row_items=row.find_all("td")
        if len(row_items) != 0:
            Name=row_items[1].get_text().strip()
            MC_USD_Billion=row_items[2].get_text().strip()
            try:
                MC_USD_Billion_float= float(MC_USD_Billion)
                data_dict={"Name":Name,"MC_USD_Billion":MC_USD_Billion_float}
                df1=pd.DataFrame(data_dict,index=[0])
                df=pd.concat([df,df1],ignore_index=True)
            except ValueError :
                print(f"we skipped row what conatain MC_USD_Billion : {MC_USD_Billion} due to un accepted value")
    return df

def transform(df,exchange_rate_csv_path):
    rate_df=pd.read_csv(exchange_rate_csv_path,delimiter=",")
    rate_dict= rate_df.set_index("Currency")["Rate"].to_dict()
    df['MC_EUR_Billion']=np.round(df['MC_USD_Billion'] * rate_dict["EUR"],2)
    df['MC_GBP_Billion']=np.round(df['MC_USD_Billion'] * rate_dict["GBP"],2)
    df['MC_INR_Billion']=np.round(df['MC_USD_Billion'] * rate_dict["INR"],2)
    df.to_csv('./largest_banks_data.csv',index= False)
    return df

def load_to_sql(table_name,sql_connection,df):
    df.to_sql(table_name,sql_connection,index=False,if_exists="replace")

def sql_query(sql_statmen,sql_connection):
    print(sql_connection)
    sql_result = pd.read_sql_query(sql_statmen,sql_connection)
    print(sql_result)


# start_work

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion'] 
db_name = 'Banks.db'
table_name = 'Largest_banks'
exchange_rate_csv_path = './exchange_rate.csv'
csv_path = './largest_banks_data.csv'  
sql_connection= sqlite3.connect(db_name)
sql_statmen=f"SELECT * FROM {table_name}"
log_message("extract is intiated")

extracted_data  = extract(url,table_attribs)

log_message("extract is finished")

log_message("transform is started")

transformed_data= transform(extracted_data,exchange_rate_csv_path)

log_message("transform is finished")

log_message("load_to_sqlis started")

load_to_sql(table_name,sql_connection,transformed_data)

sql_query(sql_statmen,sql_connection)

sql_connection.close()











