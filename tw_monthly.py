import requests
import sqlite3
import pandas as pd
from datetime import date
from io import StringIO

def monthly_crawler(date_, csv=False, sql=False, sqlpath='/Users/Reeve/Desktop/DataBase/test.sqlite'):

    year, month, day = str(date_).split('-')
    month = str(int(month))
    cyear = int(year) - 1911
    date_ = date_.strftime('%Y%m') 


    if sql==True:    
        conn = sqlite3.connect(sqlpath)
        try:
            cursor = conn.execute("select * from monthly_revenue where month='{}'AND 公司代號='1101';".format(date_)) 
            if cursor.fetchone() != None:
                print(f'monthly_revenue of {date_} already dowload into the database.')
                conn.close()
                return None
        except:
            pass

    if cyear < 102:
        url_end = ''
        start_num = 1
    else:
        url_end = '_0'
        start_num = 2

    url = f'https://mops.twse.com.tw/nas/t21/sii/t21sc03_{cyear}_{month}{url_end}.html'
    r = requests.get(url)
    r.encoding = 'big5'
    dfs = pd.read_html(StringIO(r.text))
    new_df = []

    for i in range(start_num, len(dfs), 2):
        new_df.append(dfs[i])

    df = pd.concat(new_df)
    new_label = []

    for i in range(len(df.columns)):
        new_label.append(df.columns[i][1].replace(' ',''))

    df.columns = new_label
    if cyear < 102:
        df = df.loc[~(df['公司代號'] == '合計')]
    else:
        df = df.loc[~(df['公司代號'] == '合計')].drop('備註', axis=1)

    df = df.set_index(['公司代號'])
    df.insert(0, 'month', date_)

    # save to csv
    if csv == True:
        df.to_csv('C:\\Users\\Reeve\\Desktop\\Project\\finance\\csv_data\{}_monthly.csv'.format(date_), encoding = 'utf-8-sig')

    # save to SQLite 
    if sql == True:
        df.to_sql('monthly_revenue', conn, if_exists='append')
        conn.close()

    return df


# 2. save to csv 確認格式無誤

def main():

    date_ = date(2023,11,11)
    df_monthly = monthly_crawler(date_, csv=False, sql=False)
    print(df_monthly)
    print(df_monthly.index.tolist())



if __name__ == '__main__':
    main()