# **Build a simple stock analysis Python App with MatrixOne**

This tutorial shows you how to build a simple Python application with MatrixOne.

## **About the demo**

This demo will store the historical stock data of the China stock market and make a simple analysis to find the best stocks to buy.

The basic idea is that we track the P/E (Price-to-Earnings) and P/B (Price-to-Book) level of each stock since these can represent a company's market capitalization. If you are not familiar with these two concepts, please refer to [P/E](https://www.investopedia.com/terms/p/price-earningsratio.asp) and [P/B](https://www.investopedia.com/terms/p/price-to-bookratio.asp).

Every day after the market closes, we compare the P/E and P/B value with the historical lowest P/E and P/B. If the current P/E or P/B is even lower than the historical lowest, the stock is very likely under-estimated. We can further investigate its reason of falling and judge if it's a good time to buy it. 

## **Before you start**

Before you start, you need to have a Tushare account and get an API token. If you haven't signed up for Tushare, you can sign up for it at [https://tushare.pro/](https://tushare.pro/) and you can find your API token at [https://tushare.pro/user/token#](https://tushare.pro/user/token#).

!!! info 
    Tushare is an open source utility for crawling historical data of China stocks. It has a complete dataset, but pay attention to their rules of API access limit and frequency. 

Besides the data source account, you need to at least have these two basic software installed:

* Python 3.x 
* MatrixOne

You may refer to [Python 3 installation tutorial](https://realpython.com/installing-python/) and [MatrixOne installation](../Get-Started/install-standalone-matrixone.md) for more details.

Moreover, we need to install dependant `Tushare` and `pymysql` python libraries to use Tushare and access MatrixOne. 

```bash
pip3 install -r requirements.txt
```

!!! info 
    `Pymysql` is the only ORM tool supported by MatrixOne. The other python MySQL ORM tools as `SQLAlchemy`, `mysql-connector`, `MySQLdb` are not supported yet.

## **Step1: prepare and load historical dataset**

First, we need to load the historical stock data in MatrixOne. 

As `Tushare` interface only allows a fetch of 5000 rows of data at once, we only have to collect the latest 5000 trade days data for each stock. Each year there are roughly 250 trade dates. 5000 makes almost 20 years of data, which largely satisfies our demo.

The below code will set up tushare interface and get the list of stocks.

```python
import tushare as ts
import time

# Set Tushare data source
ts.set_token('YOUR_TUSHARE_API_TOKEN')
pro = ts.pro_api()

# Get the list of stocks
pool = pro.stock_basic(exchange = '',
                        list_status = 'L',
                        adj = 'qfq',
                        fields = 'ts_code,symbol,name,area,industry,fullname,list_date, market,exchange,is_hs')
                        

```

As we only need the P/E and P/B information, we call the `daily_basic` method and get each stock with data frames with `ts_code`, `trade_date`, `pe` and `pb` fields. Without any specification of start date and end date, `Tushare` will automatically output 5000 lastest records.

```python

j = 1
for i in pool.ts_code:
    print('Getting %d stock，Stock Code %s.' % (j, i))
    #The interface is limited to be queried 200 times/minute, some little delays are necessary
    time.sleep(0.301)
    j+=1

	 #Get stock P/E and P/B data frames
    df = pro.daily_basic(**{
    "ts_code": i,
    "trade_date": "",
    "start_date": "",
    "end_date": "",
    "limit": "",
    "offset": ""
    }, fields=[
    "ts_code",
    "trade_date",
    "pe",
    "pb"
    ])

```

With the data ready, we need to load them into MatrixOne. We use `pymysql` as the python-MatrixOne connector to run SQLs in MatrixOne. Prior to the below code, the MatrixOne server must be launched and a database called `stock` needs to be created first.

MatrixOne:

```sql
mysql> CREATE DATABASE stock.

```

Python: 

```
import pymysql

# Open a MatrixOne connection
db = pymysql.connect(host='127.0.0.1',
                     port=6001,
                     user='dump',
                     password='111',
                     database='stock')
 
# Create a cursor object
cursor = db.cursor()

# Create PE table
cursor.execute('CREATE TABLE IF NOT EXISTS pe(ts_code VARCHAR(255), trade_date VARCHAR(255), pe FLOAT, pb FLOAT)') 

```

Load each 5000 records of data into MatrixOne:

```python

if df.empty == False:
		 # Fill P/E and P/B values which are NaN
        df = df.fillna(0.0)
        val_to_insert = df.values.tolist()
        cursor.executemany(" insert into pe (ts_code, trade_date,pe,pb) values (%s, %s,%s,%s)", val_to_insert)
        
```

## **Step2: find the historical lowest P/E or P/B stock**

After we load the historical stock data into MatrixOne, we have about 11 millions records. 

```sql
mysql> select count(*) from pe;
+----------+
| count(*) |
+----------+
| 11233508 |
+----------+
1 row in set (0.16 sec)
```

Now we run a sql to calculate the lowest P/E and P/B of each stock. 

```python
# Find stocks that the current P/E is even lower than the historical lowest
cursor.execute('select ts_code,min(pe) from pe where pe>0 group by ts_code order by ts_code')
# Fetch the result as python object
value = cursor.fetchall() 

# Find stocks that the current P/B is even lower than the historical lowest
cursor.execute('select ts_code,min(pb) from pe where pb>0 group by ts_code order by ts_code')
# Fetch the result as python object
value = cursor.fetchall() 
```

Then we call Tushare `daily_basic` interface again to get the current P/E and P/B level and make a search to locate the stock whose current P/E or P/B is even lower than the historical lowest. You could of course switch any trade date you want for comparison. We take P/E as an example.

```python
df = pro.daily_basic(**{
    "ts_code": "",
    "trade_date": sys.argv[1],
    "start_date": "",
    "end_date": "",
    "limit": "",
    "offset": ""
    }, fields=[
    "ts_code",
    "pe"
    ])
df = df.fillna(0.0)

for i in range(0,len(value)):
    ts_code, min_pe = value[i]

    for j in range(0, len(df.ts_code)):
        if ts_code == df.ts_code[j] and min_pe > df.pe[j] > 0:
            logging.getLogger().info("ts_code: %s", ts_code)
            logging.getLogger().info("history lowest PE : %f", min_pe)
            logging.getLogger().info("current PE found: %f", df.pe[j])
```

This will print every stock with the lowest ever P/E or P/B. They are usually good choices to buy if they are not experiencing big trouble. 

## **Step3: Update our dataset**

We already have a lot of historical data stored in MatrixOne, each time we run a n analysis, we usually have been through a new trading day. The dataset need to be updated with the latest data, otherwise next time you will still compare yourself with the old data. 

We don't need to insert all data again, we just insert the data with the lastest trading dates. 

```python
# get updating trading dates by user argument inputs
df = pro.daily_basic(**{
            "ts_code": i,
            "trade_date": "",
            "start_date": self.startDate,,
            "end_date": self.endDate,
            "limit": "",
            "offset": ""
}, fields=[
            "ts_code",
            "trade_date",
            "pe",
            "pb"
])

# append update to MatrixOne
if df.empty == False:
            df = df.fillna(0.0)
            val_to_insert = df.values.tolist()
            print(val_to_insert)
            cursor.executemany(" insert into pe (ts_code, trade_date,pe,pb) values (%s, %s,%s,%s)", val_to_insert)
            
``` 

## Source Code

You can find the source code of this demo at [matrix_one_app](https://github.com/matrixorigin/matrixone_python_app).