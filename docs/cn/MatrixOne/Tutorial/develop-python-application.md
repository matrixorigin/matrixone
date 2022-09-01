# **用 MatrixOne 构建一个简单的股票分析 Python 应用程序**

本教程将向你展示如何使用 MatrixOne 构建一个简单的 Python 应用程序。

## **关于 Demo**

本教程内所展示的 Demo 将存储中国股市的历史股票数据，并进行简单的分析，找出最适合购买的股票。

**Demo 构思**：跟踪每支股票的市盈率（PE，Price to Earnings ratio，简称 P/E 或 PER）和市净率（PB，Price to book ratio，简称 P/B 或 PBR）水平，因为市盈率和市净率水平可以代表一家公司的市值。有关市盈率和市净率的概念，参考[P/E](https://www.investopedia.com/terms/p/price-earningsratio.asp)和[P/B](https://www.investopedia.com/terms/p/price-to-bookratio.asp)。

每天股市收盘后，我们都会将市盈率和市净率值与历史最低市盈率和市净率进行比较。如果当前的市盈率低于历史最低水平，那么这支股票很可能被低估了。我们可以进一步调查它下跌的原因，判断现在是不是买入的好时机。

### Demo 源码

你可以直接下载[matrix_one_app](https://github.com/matrixorigin/matrixone_python_app)使用。

## **开始前准备**

### 准备 1：获取 Tushare API Token

- 你需要有一个 Tushare 帐户并获得一个 API Token。如果你还没有注册 Tushare，在[https://tushare.pro/](https://tushare.pro/)进行注册账户，在[https://tushare.pro/user/token#](https://tushare.pro/user/token#)获取 API Token。

!!! info
    Tushare 是一个用于爬行中国股票历史数据的开源工具。它有一个完整的数据集，但要注意 Tushare 的 API 访问限制和访问频率的规则，如需正常使用，请按照 Tushare 的 API 访问权限进行付费。

### 准备 2：安装基础软件

- 你还需要安装以下两个基础软件：

   + Python 3.x
   + MatrixOne

  更多详细信息，参见[Python 3 installation tutorial](https://realpython.com/installing-python/) and [MatrixOne 安装](../Get-Started/install-standalone-matrixone.md)。

### 准备 3：安装依赖

- （选做）下载源码并且安装 `Tushare` 依赖：

    a. 下载本篇文章所演示的 Demo 的源代码[matrix_one_app](https://github.com/matrixorigin/matrixone_python_app)。

    b. 进入到源码路径 *matrixone_python_app/stock_analysis* 安装 `Tushare` 依赖项：

    ```bash
    cd matrixone_python_app/stock_analysis
    pip3 install -r requirements.txt
    ```

- 安装依赖 [pymysql](https://github.com/PyMySQL/PyMySQL)。

!!! info
    MatrixOne 当前仅支持 Pymysql。暂不支持 SQLAlchemy，MySQL-connector 和 MySQLdb。

## **步骤 1：编写 Python，准备和加载历史数据集**

首先，在 MatrixOne 中加载历史股票数据。

Tushare 界面只允许一次读取5000行数据，所以你只需要收集每只股票最新的5000个交易日的数据。每年大约有250个交易日期。5000构成了近20年的数据，这在很大程度上满足了本次 Demo。

1. 建立 Tushare 界面并获取股票列表：

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

2. 使用调用 `daily_basic` 的方法，用 `ts_code` ，`trade_date`，`pe` 和 `pb` 字段的数据帧获取每支股票 P/E 和 P/B 的信息，那么 Tushare 将自动输出5000条最新记录，且无需指定开始日期和结束日期。

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

3. 启动 MatrixOne 服务器，在 MatrixOne 创建名为 “stock” 的数据库。

    ```sql
    mysql> CREATE DATABASE stock.
    ```

4. 将股票数据加载到 MatrixOne 中：使用 `pymysql` 作为 Python 与 MatrixOne 的连接器。

    ```python
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

5. 将每5000条数据记录加载到 MatrixOne 中：

    ```python

    if df.empty == False:
    		 # Fill P/E and P/B values which are NaN
            df = df.fillna(0.0)
            val_to_insert = df.values.tolist()
            cursor.executemany(" insert into pe (ts_code, trade_date,pe,pb) values (%s, %s,%s,%s)", val_to_insert)

    ```

## **步骤 2：找到历史最低市盈率或市盈率股票**

1. 将历史股票数据加载到 MatrixOne 后，查看数据记录，可以看到大约有1100万条记录：

    ```sql
    mysql> select count(*) from pe;
    +----------+
    | count(*) |
    +----------+
    | 11233508 |
    +----------+
    1 row in set (0.16 sec)
    ```

2. 通过 Python 计算每支股票的最低 P/E 和 P/B：

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

3. 再次调用 Tushare 的 “daily_basic” 接口，得到当前的市盈率和市盈率水平，并进行搜索，找到当前市盈率或市盈率甚至低于历史最低水平的股票。你可以切换任何你想要的交易日期进行比较。这里以市盈率为例：

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

4. 执行完以上代码，将打印出每支市盈率或市盈率最低的股票。

## **步骤 3：升级数据集**

在上述步骤中，你已经在 MatrixOne 中存储了许多历史数据，每次进行数据分析时，通常都经历了一个新的交易日。那么，数据集需要使用最新的数据进行更新，否则下一次你仍然会与旧数据进行比较。

无需再次插入所有数据，你只需插入具有最新交易日期的数据：

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
