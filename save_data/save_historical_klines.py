import time
from datetime import datetime, date, timedelta
import argparse
import dateparser
import pytz
import json
import pymysql
from .mysql.klines_table.historical_klines import create_historical_klines_table

from datetime import datetime
from binance.client import Client

# MySQL setting
mysqlserverip = "10.140.0.100"
mysqlserveruser = "root"
mysqlserverpwd = "1qaz@WSX"
database = "binance_dev"

connection = pymysql.connect(host=mysqlserverip,
                             user=mysqlserveruser,
                             password=mysqlserverpwd,
                             database=database,
                             autocommit=True)


# args setting
"""
If needs data 2 hours ago:
"python3 -m save_data.save_historical_klines -sym ETHBTC -ago 2 -ki 2h" 
If needs data from start date until today:
"python3 -m save_data.save_historical_klines -sym ETHUSDT -sd 2021-03-10 -ki 4h"
If needs data in a period of time:
"python3 -m save_data.save_historical_klines -sym ETHUSDT -sd 2021-03-10 -ed 2021-03-13 -ki 4h"
"""
parser = argparse.ArgumentParser()
parser.add_argument("-sym", "--symble", help="symble input value")
parser.add_argument("-sd", "--startdate", help="start date input value")
parser.add_argument("-ed", "--enddate", help="end date input value")
parser.add_argument("-ago", "--hago", help="hours ago input value")
parser.add_argument("-ki", "--kinterval", help="k line interval, i.e. 1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M")
args = parser.parse_args()


def date_to_readable_format(date_str):
    """Convert UTC date string to readable format
    :param date_str: date string, i.e. "2021-03-13"
    :type date_str: str
    """
    if int(date_str.split("-")[2]):
        try:
            year, month, day = int(date_str.split("-")[0]), int(date_str.split("-")[1]), int(date_str.split("-")[2])
        except Exception:
            raise Exception("Please input valid date value, i.e. 2021-03-13")
        readable_time = date(year, month, day).ctime()
        M, D, Y = readable_time.split(" ")[1], readable_time.split(" ")[2], readable_time.split(" ")[4]
        return (f"{M} {D} {Y}")
    else:
        return date_str


def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)


def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms


def get_historical_klines(symbol, interval, start_str, end_str=None):
    """Get Historical Klines from Binance
    See dateparse docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/
    If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    :param symbol: Name of symbol pair e.g BNBBTC
    :type symbol: str
    :param interval: Biannce Kline interval
    :type interval: str
    :param start_str: Start date string in UTC format
    :type start_str: str
    :param end_str: optional - end date string in UTC format
    :type end_str: str
    :return: list of OHLCV values
    """
    # create the Binance client, no need for api key
    client = Client("", "")

    # init our list
    output_data = []

    # setup the max limit
    limit = 500

    # convert interval to useful value in seconds
    timeframe = interval_to_milliseconds(interval)

    # convert our date strings to milliseconds
    start_ts = date_to_milliseconds(start_str)

    # if an end time was passed convert it
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)

    idx = 0
    # it can be difficult to know when a symbol was listed on Binance so allow start time to be before list date
    symbol_existed = False
    while True:
        # fetch the klines from start_ts up to max 500 entries or the end_ts if set
        temp_data = client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )

        # handle the case where our start date is before the symbol pair listed on Binance
        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            # append this loops data to our output data
            output_data += temp_data

            # update our start timestamp using the last value in the array and add the interval timeframe
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            # it wasn't listed yet, increment our start date
            start_ts += timeframe

        idx += 1
        # check if we received less than the required limit and exit the loop
        if len(temp_data) < limit:
            # exit the while loop
            break

        # sleep after every 3rd call to be kind to the API
        if idx % 3 == 0:
            time.sleep(1)

    return output_data


def ETHBTE_test():
    symbol = "ETHBTC"
    start = "1 Dec, 2017"
    end = "1 Jan, 2018"
    interval = Client.KLINE_INTERVAL_30MINUTE

    klines = get_historical_klines(symbol, interval, start, end)

    # open a file with filename including symbol, interval and start and end converted to milliseconds
    with open(
        "Binance_{}_{}_{}-{}.json".format(
            symbol,
            interval,
            date_to_milliseconds(start),
            date_to_milliseconds(end)
        ),
        'w'  # set file write mode
    ) as f:
        f.write(json.dumps(klines))
    print(klines)


def create_mysql_table(sql):
    create_connection = pymysql.connect(host=mysqlserverip,
                                        user=mysqlserveruser,
                                        password=mysqlserverpwd,
                                        autocommit=True)
    with create_connection.cursor() as cursor:
        print(f"Craete table")
        print(sql)
        cursor.execute(sql)
        create_connection.commit()


def insert_mysql_table(sql, table_name, klines):
    with connection.cursor() as cursor:
        print(f"Insert table name = {table_name}")
        # print(klines)
        print(sql)
        cursor.executemany(sql, klines)  # Insert fetch klins to mysql table
        connection.commit()


def main():
    symbol = args.symble    # i.e.ETCUSDT
    start = datetime.strptime(args.startdate, "%Y-%m-%d")  # i.e."2021-03-13"
    end = datetime.strptime(args.enddate, "%Y-%m-%d") + timedelta(days=1) if args.enddate else datetime.today() + timedelta(days=1)
    ago = f"{args.hago} hours ago UTC"    # i.e. 3
    interval = args.kinterval    # i.e. 4h

    table_schema = f"open_time,open, high, low, close,volume, close_time, quote_asset, trades, " \
                   f"taker_buy_base_asset_volume,taker_quote,can_be_ignored"
    table_name = f"{symbol}_{interval}"
    sql = f"insert into {table_name} ({table_schema}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
          f"ON duplicate KEY UPDATE open_time = open_time"

    if args.hago:
        klines = get_historical_klines(symbol, interval, ago)
        klines = klines[:-1]  # [-1] is because get_klines function will fetch first data in next date
        # print(klines)
        print(f"Fetch {len(klines)} kline")
        insert_mysql_table(sql, table_name, klines)
    elif interval in ['2d', '3d', '1w', '1M']:
        klines = get_historical_klines(symbol, interval, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
        klines = klines[:-1]  # [-1] is because get_klines function will fetch first data in next date
        print(f"Fetch {len(klines)} kline")
        insert_mysql_table(sql, table_name, klines)
    else:
        print(create_historical_klines_table(args.symble, args.kinterval))
        create_mysql_table(create_historical_klines_table(args.symble, args.kinterval))    # Create table if not exist

        for n in range(int((end - start).days)):    # Calculate the date and run one by one
            start_out = (start + timedelta(n)).strftime('%Y-%m-%d')
            end_out = (start + timedelta(n) + timedelta(1)).strftime('%Y-%m-%d')
            print(f"Fetch {start_out} data...")
            klines = get_historical_klines(symbol, interval, start_out, end_out)
            klines = klines[:-1]    # [-1] is because get_klines function will fetch first data in next date
            # print(klines[:-1])
            print(f"Fetch {len(klines)} kline")

            insert_mysql_table(sql, table_name, klines)


if __name__ == '__main__':
    # ETHBTE_test()
    main()

    # from .mysql.klines_table.historical_klines import create_historical_klines_table
    # create_mysql_table(create_historical_klines_table(args.symble, args.kinterval))
