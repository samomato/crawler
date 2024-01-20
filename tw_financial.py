import requests
import re
import sqlite3
import time
import warnings
import os
import pandas as pd
from tw_monthly import monthly_crawler
from datetime import date
# from io import StringIO


'''
年份：2019第一季開始網頁有改，要切換爬法；2018第一季開始表格內容有異動，最多找到2013年。

統一方法：將舊式名稱改成新制，新制以2330 2023第一季財報為範例，把新的取代舊的。

爬蟲方法： 直接到財報網頁會找到同樣的網址：https://mops.twse.com.tw/server-java/t164sb01。必須加上'?' 接以下網址
          此網址找法： Network > 左欄Name第一個 > Payload > view source
          Ex. https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID=1101&SYEAR=2019&SSEASON=3&REPORT_ID=C
          step: 1 （不知做啥用的）
          CO_ID: 1101 （股票代號）
          SYEAR: 2019 （年）
          SSEASON: 3 （季）
          REPORT_ID：個別財報(A) 個體財報(B) 合併報表(C)。以合併優先（90%以上的財報都是合併財報），沒有->合併，再沒有->個體
'''

warnings.filterwarnings('ignore')


def financial_crawler(year, season, company, csv=False, sql=False, sqlpath='/Users/Reeve/Desktop/DataBase/test.sqlite'):

    # balance的統一格式： 新版改動：應付帳款關係人 > 應付帳款關係人合計    其他非流動資產其他 > 其他非流動資產其他合計, 權益總計 > 權益總額。 額外加: 存貨製造業  
    blc_std = ['現金及約當現金', '透過損益按公允價值衡量之金融資產流動', '透過其他綜合損益按公允價值衡量之金融資產流動', '備供出售金融資產流動',
           '持有至到期日金融資產流動','按攤銷後成本衡量之金融資產流動', '避險之金融資產流動', '應收帳款淨額', '應收帳款關係人淨額',
           '其他應收款關係人', '存貨製造業', '存貨', '其他流動資產', '其他金融資產流動', '其他流動資產其他', '流動資產合計', 
           '透過其他綜合損益按公允價值衡量之金融資產非流動','持有至到期日金融資產非流動','以成本衡量之金融資產非流動',
           '按攤銷後成本衡量之金融資產非流動', '採用權益法之投資', '不動產廠房及設備', '使用權資產', '無形資產', '遞延所得稅資產',
           '其他非流動資產', '存出保證金', '其他非流動資產其他','其他非流動資產其他合計', '非流動資產合計', '資產總計', '短期借款',
           '透過損益按公允價值衡量之金融負債流動', '避險之金融負債流動', '應付帳款', '應付帳款關係人合計', '其他應付款', '應付薪資',
           '應付員工紅利', '應付董監事酬勞', '應付設備款', '應付股利', '本期所得稅負債', '其他流動負債','負債準備流動',
           '一年或一營業週期內到期長期負債', '其他流動負債其他', '流動負債合計', '應付公司債', '長期借款', '銀行長期借款',
           '遞延所得稅負債', '租賃負債非流動', '其他非流動負債', '淨確定福利負債非流動', '存入保證金', '其他非流動負債其他',
           '非流動負債合計', '負債總計', '普通股股本', '待註銷股本', '股本合計', '資本公積發行溢價', '資本公積普通股股票溢價',
           '資本公積轉換公司債轉換溢價', '資本公積實際取得或處分子公司股權價格與帳面價值差額', '資本公積認列對子公司所有權權益變動數',
           '資本公積受贈資產', '資本公積受領股東贈與', '資本公積其他受贈資產', '資本公積採用權益法認列關聯企業及合資股權淨值之變動數',
           '資本公積合併溢額', '資本公積限制員工權利股票', '資本公積合計', '法定盈餘公積', '特別盈餘公積', '未分配盈餘或待彌補虧損',
           '保留盈餘合計', '其他權益合計', '庫藏股票', '歸屬於母公司業主之權益合計', '非控制權益', '權益總額', '負債及權益總計',
           '待註銷股本股數', '預收股款權益項下之約當發行股數', '母公司暨子公司所持有之母公司庫藏股股數單位股']

    # income的統一格式：
    income_std=['營業收入合計', '營業成本合計', '營業毛利毛損', '營業毛利毛損淨額', '推銷費用', '管理費用', '研究發展費用',
           '營業費用合計', '其他收益及費損淨額', '營業利益損失', '銀行存款利息', '按攤銷後成本衡量之金融資產利息收入',
           '透過其他綜合損益按公允價值衡量之金融資產利息收入', '利息收入合計', '其他收入其他', '其他收入合計', '其他利益及損失淨額',
           '財務成本淨額', '採用權益法認列之關聯企業及合資損益之份額淨額', '營業外收入及支出合計', '繼續營業單位稅前淨利淨損',
           '所得稅費用利益合計', '繼續營業單位本期淨利淨損', '本期淨利淨損', '透過其他綜合損益按公允價值衡量之權益工具投資未實現評價損益',
           '避險工具之損益不重分類至損益', '採用權益法認列之關聯企業及合資之其他綜合損益之份額不重分類至損益之項目',
           '與不重分類之項目相關之所得稅', '不重分類至損益之項目總額', '國外營運機構財務報表換算之兌換差額',
           '透過其他綜合損益按公允價值衡量之債務工具投資未實現評價損益', '避險工具之損益',
           '採用權益法認列之關聯企業及合資之其他綜合損益之份額可能重分類至損益之項目', '與可能重分類之項目相關之所得稅',
           '後續可能重分類至損益之項目總額', '其他綜合損益淨額', '本期綜合損益總額', '母公司業主淨利損', '非控制權益淨利損',
           '母公司業主綜合損益', '非控制權益綜合損益', '基本每股盈餘合計', '稀釋每股盈餘合計']

    en_letter = '[\u0041-\u005a|\u0061-\u007a]+' # 大小寫英文字母
    zh_char = '[\u4e00-\u9fa5]+' # 中文字符
    year = str(year)
    season = str(season)
    date_ = year+'-'+season

    if int(year) < 2013:
        print('can only download 2013 and later.')
        return None
    elif int(year) < 2019:  # old web
        # 沒當季資料夾(不存在)就創一個
        path_dir = f'C:\\Users\\Reeve\\Desktop\\DataBase\\FR\\{date_}'
        if not os.path.exists(path_dir):
            os.mkdir(path_dir)

        path_ = path_dir + f'\{company}.html'  # 公司財報path
        
        if not os.path.isfile(path_):  # 沒有此公司資料就爬
            try:
                r = requests.get(f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={company}&SYEAR={year}&SSEASON={season}&REPORT_ID=C')
            except:
                print('crawling error from company:{} in {}, may be url wrong, please check.'.format(company, date_))
                return None

            if len(r.text) < 6000:
                time.sleep(5)
                print(f'{company} in {date_}無合併財報')
                r = requests.get(f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={company}&SYEAR={year}&SSEASON={season}&REPORT_ID=A')
                if len(r.text) < 6000:
                    time.sleep(5)
                    print(f'{company} in {date_}無個別財報')
                    r = requests.get(f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={company}&SYEAR={year}&SSEASON={season}&REPORT_ID=B')
                    if len(r.text) < 6000:
                        print('No any finance report')
                        return None
            print(len(r.text)) #
            r.encoding = 'big5'
            f = open(path_, 'w', encoding='UTF-8')
            f.write(r.text)
            f.close()
            time.sleep(30)
        else:
            print(f"{company} in season {date_} is exist, don't need to download.")

        #  開始處理/讀入資料
        with open(path_,'r', encoding='UTF-8') as fr:
            data = fr.read().replace("<br>",'\n')
        
        dfs = pd.read_html(data.replace('(','-').replace(')',''))  # dfs = pd.read_html(StringIO(r.text))
        df_balance = dfs[1]
        df_income = dfs[2]
        df_cash_flows = dfs[3]
        # 轉換成str後才能把nan去掉
        df_balance = df_balance.iloc[:,:2].astype(str)
        df_income = df_income.iloc[:,:2].astype(str)
        df_cash_flows = df_cash_flows.iloc[:,:2].astype(str)
        # tuple改成數字後面好呼叫
        df_balance.columns = [0, 1]
        df_income.columns = [0, 1]
        df_cash_flows.columns = [0, 1]

        # 再把大項(後面會有nan)的刪除後做轉置
        df_balance = df_balance.loc[~(df_balance[1] == 'nan')]
        i = 0
        new_balance = []
        for c in df_balance[0]:
            df_balance[0].iloc[i] = ''.join(re.findall(zh_char,c))  # 只留中文字串
            if int(year) < 2018:
                Temp = df_balance[0].iloc[i]
                flag = 0
                j = 0
                for s in blc_std:
                    if j < i-7:
                        j+=1
                        continue
                    if s == df_balance[0].iloc[i]: 
                        Temp = s
                        break
                    elif s in df_balance[0].iloc[i]:
                        flag += 1
                        if flag==1:
                            Temp = s
                df_balance[0].iloc[i] = Temp
            elif int(year) >= 2018:
                if df_balance[0].iloc[i] == '應付帳款關係人':
                    df_balance[0].iloc[i] = '應付帳款關係人合計'
                elif df_balance[0].iloc[i] == '其他非流動資產其他':
                    df_balance[0].iloc[i] = '其他非流動資產其他合計'
                elif df_balance[0].iloc[i] == '權益總計':
                    df_balance[0].iloc[i] = '權益總額'

            if df_balance[0].iloc[i] not in new_balance:
                new_balance.append(df_balance[0].iloc[i])
            else:
                new_balance.append(df_balance[0].iloc[i]+'二')
            i+=1
        df_balance[0] = new_balance
        df_balance = df_balance.T
        df_balance.columns = df_balance.iloc[0]
        df_balance = df_balance.drop(0)
        df_balance.insert(0, '公司代號',company)
        df_balance.index = [date_]
        df_balance.index.name = '季別'
    #--------------------------------綜合損益表---------------------------------------------------
        new_income = []
        df_income = df_income.loc[~(df_income[1] == 'nan')]
        i = 0
        for c in df_income[0]:
            df_income[0].iloc[i] = ''.join(re.findall(zh_char,c))  # 只留中文字串
            if int(year) < 2018:
                Temp = df_income[0].iloc[i]
                flag = 0
                j = 0
                for s in income_std:
                    if j < i-3:
                        j+=1
                        continue
                    if s == df_income[0].iloc[i]: 
                        Temp = s
                        break
                    elif s in df_income[0].iloc[i]:
                        flag += 1
                        if flag==1:
                            Temp = s
                df_income[0].iloc[i] = Temp

            if df_income[0].iloc[i] not in new_income:
                new_income.append(df_income[0].iloc[i])
            elif df_income[0].iloc[i]+'二' in new_income:
                new_income.append(df_income[0].iloc[i]+'三')
            else:
                new_income.append(df_income[0].iloc[i]+'二')


            i+=1
        df_income[0] = new_income
        df_income = df_income.T
        df_income.columns = df_income.iloc[0]
        df_income = df_income.drop(0)
        df_income.insert(0, '公司代號',company)
        df_income.index = [date_]
        df_income.index.name = '季別'

    # -------------------------------現金流量表----------------------------------------------------
        df_cash_flows = df_cash_flows.loc[~(df_cash_flows[1] == 'nan')]
        new_cash = []
        
        for s in df_cash_flows[0]:
            s = ''.join(re.findall(zh_char,s))

            if s not in new_cash:
                new_cash.append(s)
            else:
                new_cash.append(s+'二')

        df_cash_flows[0] = new_cash
        df_cash_flows = df_cash_flows.T
        df_cash_flows.columns = df_cash_flows.iloc[0]
        df_cash_flows = df_cash_flows.drop(0)
        df_cash_flows.insert(0, '公司代號',company)
        df_cash_flows.index = [date_]
        df_cash_flows.index.name = '季別'
        # df_balance.apply(lambda s: pd.to_numeric(s, errors='coerce'))
        print('complete crawling from company:{} in {}!'.format(company, date_))

    else:
        # 以下開始看2019年開始後的爬法:
        path_dir = f'C:\\Users\\Reeve\\Desktop\\DataBase\\FR\\{date_}'
        if not os.path.exists(path_dir): 
            os.mkdir(path_dir)
        path_ = path_dir + f'\{company}.html'
        if not os.path.isfile(path_):
            try:
                r = requests.get(f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={company}&SYEAR={year}&SSEASON={season}&REPORT_ID=C')
            except:
                print('crawling error from company:{} in {}, may be url wrong, please check.'.format(company, date_))
                return None

            print(len(r.text))
            if len(r.text) < 150:
                time.sleep(5)
                print(f'{company} in {date_}無合併財報')
                r = requests.get(f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={company}&SYEAR={year}&SSEASON={season}&REPORT_ID=A')
                if len(r.text) < 150:
                    time.sleep(5)
                    print(f'{company} in {date_}無個別財報')
                    r = requests.get(f'https://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID={company}&SYEAR={year}&SSEASON={season}&REPORT_ID=B')
                    if len(r.text) < 100:
                        print('No any finance report')
                        return None
            r.encoding = 'big5'
            f = open(path_, 'w', encoding='UTF-8')
            f.write(r.text)
            f.close()
            time.sleep(30)
        else:
            print(f"{company} in season {date_} is exist, don't need to download.")

        with open(path_,'r', encoding='UTF-8') as fr:
            data = fr.read().replace("<br>",'\n')

        dfs = pd.read_html(data.replace('(','-').replace(')',''))
        df_balance = dfs[0]
        df_income = dfs[1]
        df_cash_flows = dfs[2]
        df_balance = df_balance.iloc[:,1:3].astype(str)
        df_income = df_income.iloc[:,1:3].astype(str)
        df_cash_flows = df_cash_flows.iloc[:,1:3].astype(str)
        df_balance.columns = [0, 1]
        df_income.columns = [0, 1]
        df_cash_flows.columns = [0, 1]
        df_balance = df_balance.loc[~(df_balance[1] == 'nan')]
        i = 0
        new_balance = []
        for s in df_balance[0]:
            df_balance[0].iloc[i] = ''.join(re.findall(zh_char,s))
            if df_balance[0].iloc[i] == '應付帳款關係人':
                df_balance[0].iloc[i] = '應付帳款關係人合計'
            elif df_balance[0].iloc[i] == '其他非流動資產其他':
                df_balance[0].iloc[i] = '其他非流動資產其他合計'
            elif df_balance[0].iloc[i] == '權益總計':
                df_balance[0].iloc[i] = '權益總額'
            
            if df_balance[0].iloc[i] not in new_balance:
                new_balance.append(df_balance[0].iloc[i])
            else:
                new_balance.append(df_balance[0].iloc[i]+'二')
            i+=1
        df_balance[0] = new_balance
        df_balance = df_balance.T
        df_balance.columns = df_balance.iloc[0]
        df_balance = df_balance.drop(0)
        df_balance.insert(0, '公司代號',company)
        df_balance.index = [date_]
        df_balance.index.name = '季別'
    #--------------------------------綜合損益表---------------------------------------------------
        new_income = []
        df_income = df_income.loc[~(df_income[1] == 'nan')]
        i = 0
        for s in df_income[0]:
            df_income[0].iloc[i] = ''.join(re.findall(zh_char,s))
            
            if df_income[0].iloc[i] not in new_income:
                new_income.append(df_income[0].iloc[i])
            else:
                new_income.append(df_income[0].iloc[i]+'二')
            i+=1
        df_income[0] = new_income
        df_income = df_income.T
        df_income.columns = df_income.iloc[0]
        df_income = df_income.drop(0)
        df_income.insert(0, '公司代號',company)
        df_income.index = [date_]
        df_income.index.name = '季別'
    # -------------------------------現金流量表---------------------------------------------------
        df_cash_flows = df_cash_flows.loc[~(df_cash_flows[1] == 'nan')]
        new_cash = []

        for s in df_cash_flows[0]:
            s = ''.join(re.findall(zh_char,s))

            if s not in new_cash:
                new_cash.append(s)
            else:
                new_cash.append(s+'二')

        df_cash_flows[0] = new_cash    
        df_cash_flows = df_cash_flows.T
        df_cash_flows.columns = df_cash_flows.iloc[0]
        df_cash_flows = df_cash_flows.drop(0)
        df_cash_flows.insert(0, '公司代號',company)
        df_cash_flows.index = [date_]
        df_cash_flows.index.name = '季別'
        print('complete crawling from company:{} in {}!'.format(company, date_))

    if csv == True:
        df_balance.to_csv('C:\\Users\\Reeve\\Desktop\\Project\\finance\\csv_data\\balance_{}_{}_FR.csv'.format(company, date_), encoding = 'utf-8-sig')
        df_cash_flows.to_csv('C:\\Users\\Reeve\\Desktop\\Project\\finance\\csv_data\\cash_{}_{}_FR.csv'.format(company, date_), encoding = 'utf-8-sig')
        df_income.to_csv('C:\\Users\\Reeve\\Desktop\\Project\\finance\\csv_data\\income_{}_{}_FR.csv'.format(company, date_), encoding = 'utf-8-sig')

    if sql == True:
        conn = sqlite3.connect('/Users/Reeve/Desktop/DataBase/test.sqlite')
        c = conn.cursor()
        # -------------------------write in balance sheet-----------------------------------
        try:
            no_table = 0
            c.execute(f"SELECT 公司代號 FROM 'balance' WHERE 公司代號='{company}' AND 季別='{date_}'")
        except sqlite3.OperationalError:
            no_table = 1
        result = c.fetchall()
        if no_table==0 and result==[]:  # table exist and new row
            c.execute('PRAGMA TABLE_INFO(balance)')
            sql_b_cn = [tup[1] for tup in c.fetchall()]
            for df_cn in df_balance.columns:
                if df_cn not in sql_b_cn:
                    c.execute("ALTER TABLE 'balance' ADD COLUMN {cn}".format(cn=df_cn))
            conn.commit()
            df_balance.to_sql('balance', conn, if_exists='append')
            print(f'Had written {date_} {company} into balance table')
        elif no_table==1:
            print(f'no balance table, create new one with {date_} {company}')
            df_balance.to_sql('balance', conn, if_exists='append')
        else:
            print(f'Table balance already exist {date_} {company}')

        # -------------------------write in income sheet-----------------------------
        try:
            no_table = 0
            c.execute(f"SELECT 公司代號 FROM 'income' WHERE 公司代號='{company}' AND 季別='{date_}'")
        except sqlite3.OperationalError:
            no_table = 1
        result = c.fetchall()
        if no_table==0 and result==[]:  # table exist and new row
            c.execute('PRAGMA TABLE_INFO(income)')
            sql_in_cn = [tup[1] for tup in c.fetchall()]
            for df_cn in df_income.columns:
                if df_cn not in sql_in_cn:
                    c.execute("ALTER TABLE 'income' ADD COLUMN {cn}".format(cn=df_cn))
            conn.commit()
            df_income.to_sql('income', conn, if_exists='append')
            print(f'Had written {date_} {company} into income table')
        elif no_table==1:
            print(f'no income table, create new one with {date_} {company}')
            df_income.to_sql('income', conn, if_exists='append')
        else:
            print(f'Table income already exist {date_} {company}')

        # -----------------------write in cash-flow sheet--------------------------------
        try:
            no_table = 0
            c.execute(f"SELECT 公司代號 FROM 'cash_flows' WHERE 公司代號='{company}' AND 季別='{date_}'")
        except sqlite3.OperationalError:
            no_table = 1
        result = c.fetchall()
        if no_table==0 and result==[]:  # table exist and new row
            c.execute('PRAGMA TABLE_INFO(cash_flows)')
            sql_cash_cn = [tup[1] for tup in c.fetchall()]
            for df_cn in df_cash_flows.columns:
                if df_cn not in sql_cash_cn:
                    c.execute("ALTER TABLE 'cash_flows' ADD COLUMN {cn}".format(cn=df_cn))
            conn.commit()
            df_cash_flows.to_sql('cash_flows', conn, if_exists='append')
            print(f'Had written {date_} {company} into cash_flows table')
        elif no_table==1:
            print(f'no cash_flows table, create new one with {date_} {company}')
            df_cash_flows.to_sql('cash_flows', conn, if_exists='append')
        else:
            print(f'Table cash_flows already exist {date_} {company}')
 
        conn.close()

    return [df_balance, df_income, df_cash_flows]


def main():
    year = 2014
    season = 1
    print('Downloading Taiwan stock 2018 season 1 財報...')
    # 找到此季所有公司的代號，用月營收function去找，第1季->1月；第2季->4月；第3季->7月；第4季->10月
    month = season*3-2
    date_for_monthly = date(year,month,11)
    df_seed = monthly_crawler(date_for_monthly)
    seed = df_seed.index.tolist()
    i = 0
    length = len(seed)
    for com in seed:
        i += 1
        financial_crawler(year, season, com, sql=True)
        print(str(round(i/length*100, 3))+'%', 'No.{}'.format(i), '\n')

    # For test only:
    # dffr = financial_crawler(2018, 1, '2114', sql=True)

if __name__ == '__main__':
    main()


