import requests
import sqlite3
import pandas as pd
from io import StringIO
from datetime import date

def price_crawler(date_, nan_cancel=True, 
                csv=False, 
                sql=False, sqlpath='/Users/Reeve/Desktop/DataBase/test.sqlite'):
    """
    input the python datetime.date format/
    if one stock has no value, will show nan eventually, nan_cancel = True will store the stock with price/value only.
    csv = TrueTrue will ouput a csv
    sql = True will store into sql
    Return a Dataframe of the date
    """

    year, month, day = str(date_).split('-')
    date_ = date_.strftime('%Y%m%d') 

    if sql==True:    
        conn = sqlite3.connect(sqlpath)
        try:
            cursor = conn.execute("select * from price where date='{}'AND 證券代號='1101';".format(date_))
            if cursor.fetchone() != None:
                print(f'price of {date_} already dowload into the database.')
                conn.close()
                return None
        except:
            pass

    url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={date_}&type=ALLBUT0999&response=csv&_=1700894396517'  # modified json to csv
    res = requests.get(url)  # res.encoding = 'utf-8' ; may need to add try except here. 
    if res.text == '':
        print('No data for {}/{}/{}. It may be Taiwan Holiday.'.format(year, month, day))
        return None
    lines = res.text.split('\n')
    stock_lines = []
    for l in lines:
        length = len(l.split('",'))
        if length > 16:
            stock_lines.append(l)  # only retain 

    stock_csv = "\n".join(stock_lines)  # opposite of split
    stock_csv = stock_csv.replace('=','')
    df = pd.read_csv(StringIO(stock_csv))
    df = df.astype(str)

    df = df.apply(lambda s:s.str.replace(',',''))
    df = df.set_index('證券代號')
    df = df.apply(lambda s: pd.to_numeric(s, errors='coerce'))  # coerce means show NaN if failed  # 沒有交易量的股票顯示"--"，轉譯過就變成NaN

    remain_label = df.columns[df.isnull().sum() != len(df)]
    df = df[remain_label]  # clear up whole NaN columns
    df.insert(0, 'date', date_)
    df_no_nan = df.loc[~df['收盤價'].isnull()]  # clear up whole 收盤價 NaN rows

    if nan_cancel == True:
        df = df_no_nan

    # save to csv
    if csv == True:
        df.to_csv('C:\\Users\\Reeve\\Desktop\\Project\\finance\\csv_data\{}_price.csv'.format(date_), encoding = 'utf-8-sig')
        # df_read = pd.read_csv('{}_price.csv'.format(date_), index_col=['證券代號'])


    # save to SQLite 
    if sql == True:
        # conn = sqlite3.connect('/Users/Reeve/Desktop/DataBase/test.sqlite')
        df.to_sql('price', conn, if_exists='append')

    # df = pd.read_sql('select * from daily_price_2', conn, index_col=['證券代號'])
        conn.close()

    return df


if __name__ == '__main__':
    date_ = date(2023,11,29)
    print(price_crawler(date_, nan_cancel=True, csv=True, sql=False))
