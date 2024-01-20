import requests
import pandas as pd
import sqlite3
from io import StringIO 
from bs4 import BeautifulSoup
from datetime import date, timedelta



def futures_crawler(date_, csv=False, sql=False, sqlpath='/Users/Reeve/Desktop/DataBase/test.sqlite'):

     
    year, month, day = str(date_).split('-')
    
    if sql==True:    
        conn = sqlite3.connect(sqlpath)
        try:
            cursor = conn.execute("select * from futures where date='{}'AND 商品='臺股期貨';".format(date_))
            if cursor.fetchone() != None:
                print(f'price of {date_} already dowload into the database.')
                conn.close()
                return None
        except:
            pass

    r = requests.get(f'https://www.taifex.com.tw/cht/3/futContractsDate?queryType=1&doQuery=1&queryDate={year}%2F{month}%2F{day}')
    if r.status_code == requests.codes.ok:
        soup = BeautifulSoup(r.text, 'html.parser')
        # print(soup.prettify())
        trs = soup.find_all('tr', class_='12bk')    # 還沒解決當網頁沒data時秀出except訊息
        data_date = []
        if trs == []: 
            print('No data for {}/{}/{}. It may be Taiwan Holiday.'.format(year, month, day))
            # return data_date
            return None

        trs_label = trs[:2]
        rows = trs[3:]
        


        for row in rows:
            names = row.find_all('th')
            cells = [name.text.strip() for name in names]
            nums = row.find_all('td')
            if cells[0] == '期貨小計':
                break
            if len(cells) > 1:
                product = [cells[1]]
                cells = cells[1:] + [num.text.strip() for num in nums]
            else:
                cells = product + cells + [num.text.strip() for num in nums]

            converted = [int(cell.replace(',','')) for cell in cells[2:]]
            data_row = cells[:2] + converted

            data_date.append(data_row)
        
        new_label = ['商品', '法人', '交易多方口數','交易多方金額','交易空方口數','交易空方金額','交易淨額口數','交易淨額金額',
        '未平倉多方口數','未平倉多方金額','未平倉空方口數','未平倉空方金額','未平倉淨額口數','未平倉淨額金額']

        df = pd.DataFrame(data_date)
        df.columns = new_label
        df = df.set_index(['商品','法人'])
        df.insert(0, 'date', date_)

        if csv == True:
            df.to_csv('C:\\Users\\Reeve\\Desktop\\Project\\finance\\csv_data\{}_futures.csv'.format(date_), encoding = 'utf-8-sig')
            
        # save to SQLite 
        if sql == True:
            df.to_sql('futures', conn, if_exists='append')
            conn.close()


        print(df)
        print('successfully get data from {}/{}/{}'.format(year, month, day))
    else:
        print('connection error from {}/{}/{}'.format(year, month, day))

    return df



def main():

#    today = date.today()
    date_ = date(2020,12,11)
    delta = timedelta(days=1)
    craw_days = 1

    # for i in range(craw_days):

    #     if i == 0:
    #         pass
    #     else:
    #         today = today - delta

        # print(futures_crawler(today))
    print(futures_crawler(date_, csv=True, sql=False))


if __name__ == '__main__':
    main()
