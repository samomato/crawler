import requests
import sqlite3
import pandas as pd


conn = sqlite3.connect('/Users/Reeve/Desktop/DataBase/test.sqlite')
c = conn.cursor()
c.execute("SELECT 公司代號 FROM 'balance' WHERE 公司代號='1101' AND 季別='2023-1'")
c.fetchall()
conn.close()