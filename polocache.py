# -*- coding: utf-8 -*-

from influxdb import DataFrameClient
from dateutil.relativedelta import relativedelta

import argparse
import requests
import logging
import time

import datetime as dt
import pandas as pd

import pprint


class PoloCacheError(Exception):
    pass


class PoloCache(object):
    """
    Poloniex OHLC cache stored in InfluxDB.
    """
    BASE_URL = 'https://poloniex.com/public?command='
    PERIODS = [300, 900, 1800, 7200, 14400, 86400]

    def __init__(self, host, port):
        """
        Get list of supported assets.
        """
        self.host = host
        self.port = port
        self.ticker = PoloCache.__return_ticker()
        self.pairs = self.ticker.keys()
        print self.pairs
        assert len(self.pairs) > 0

    @staticmethod
    def __return_ticker():
        """
        Return Poloniex ticker.

        :return: ticker
        :rtype: json
        """
        return requests.get('%sreturnTicker' % PoloCache.BASE_URL).json()

    def __can_sync(self, pair, sync_frozen):
        """
        Decide whether to sync a pair or not.

        :param pair: pair to sync
        :param sync_frozen: sync even if frozen

        :return: True if to sync, False otherwise
        """
        if bool(self.ticker[pair]['isFrozen']) and not sync_frozen:
            return False
        else:
            return True

    @staticmethod
    def __create_db_if_does_not_exist(client, db_name):
        """
        Create DB if does not exist yet.

        :param db_name: database name
        :param client: InfluxDB client object
        :type client ::py:class:DataFrameClient
        :raises ::py::class:PoloCacheError if DB could not be created
        """
        dbs = [db['name'] for db in client.get_list_database()]
        if db_name not in dbs:
            logging.info("Creating database: %s" % db_name)
            client.create_database(db_name)

    def __sync(self, pair):
        """
        Sync selected pair to InfluxDb.

        :param pair: pair name e.g. BTC_ETH
        :return:
        """
        user = 'root'
        password = 'root'
        db_name = 'poloniex'
        protocol = 'json'

        client = DataFrameClient(self.host, self.port, user, password, db_name)

        try:
            PoloCache.__create_db_if_does_not_exist(client, db_name)
        except requests.exceptions.ConnectionError:
            raise PoloCacheError("Could not connect to InfluxDB on %s:%s" %
                                 (self.host, self.port))

        for period in PoloCache.PERIODS:
            t = TimeSeries()
            influxdb_series = "%s_%s" % (pair, period)

            polo_start = dt.datetime.strptime("01/05/2014", "%d/%m/%Y")

            start_month = polo_start
            end_month = polo_start + relativedelta(months=1)

            while end_month <= dt.datetime.now():
                q = client.query("select LAST(\"close\") from %s" % influxdb_series)
                if q:
                    print "Last candle cached", q[influxdb_series].index.to_series()[0]
                    start_month = q[influxdb_series].index.to_series()[0]
                    end_month = start_month + relativedelta(months=1)

                max_candles = int((end_month-start_month).total_seconds() / period)
                print("Pair %s max candles %d for period %d" % (pair, max_candles, period))

                q = client.query("select * from %s where time >= '%s' and time < '%s';"
                                 % (influxdb_series, start_month, end_month))
                if q:
                    print("Available %d candles for %s in InfluxDB for period %d" %
                          (len(q[influxdb_series].index), pair, period))
                    if len(q[influxdb_series].index) > 0.9 * max_candles:
                        print "Data already cached. Continue."
                        start_month = end_month
                        end_month = end_month + relativedelta(months=1)
                        continue

                if (dt.datetime.now() - start_month).total_seconds() >= period:
                    t.getData("%s" % pair, period, start_month, end_month)

                if not t.empty and len(t.data) > 1:
                    print("Writing %d candles from %s to %s." % (len(t.data), start_month, end_month))
                    client.write_points(t.data, influxdb_series, protocol=protocol)
                else:
                    print("No data available for this period.")

                start_month = end_month
                end_month = end_month + relativedelta(months=1)

    def sync(self, sync_frozen=False):
        """
        Sync poloniex db to a local InfluxDB instance.

        :param sync_frozen: sync frozen pair. Default: False
        :type sync_frozen: bool

        :return: number of candles inserted to InfluxDB instance
        :rtype: int

        :raises py::class:SyncError if sync failed.
        """
        for pair in self.pairs:
            print "Can sync", pair, self.__can_sync(pair, True)
            if self.__can_sync(pair, True):
                self.__sync(pair)


class TimeSeries(object):

    def __init__(self):
        self.empty = True
        self.data = None
        self.pair = ("None", "None")
        self.period = "None"
        self.start = "None"
        self.end = "None"

    def getData(self, pair, period, start, end):
        fields = ['date', 'open', 'low', 'high', 'close',
                  'weightedAverage', 'volume', 'quoteVolume']

        url = self.build_url(pair, period, start, end)
        max_attempts = 3
        for i in range(1, max_attempts):
            try:
                datapoints = requests.get(url).json()
                break
            except requests.exceptions.ChunkedEncodingError as e:
                print("%s" % e)
                time.sleep(1)

        print(url)

        data = []
        for dtp in datapoints:
            row = []
            for fld in fields:
                if fld == 'date':
                    row.append(self.toDate(dtp[fld]))
                elif fld in {'volume', 'quoteVolume', 'open', 'high', 'low', 'close'}:
                    row.append(float(dtp[fld]))
                else:
                    row.append(dtp[fld])
            data.append(row)

        temp_df = pd.DataFrame(data, columns=fields)
        temp_df.index = pd.to_datetime(temp_df['date'], dayfirst=True)
        temp_df.drop('date', axis=1, inplace=True)

        self.data = temp_df
        self.empty = False
        self.pair = pair
        self.period = period
        self.start = start
        self.end = end

    def build_url(self, pair, period, start, end):
        return '%sreturnChartData&currencyPair=%s&start=%s&end=%s&period=%s' % \
               (PoloCache.BASE_URL, pair, self.toUnix(start), self.toUnix(end), period)

    def toUnix(self, stringdate):
        #date = dt.datetime.strptime(stringdate, "%d/%m/%Y")
        return int(time.mktime(stringdate.timetuple()))

    def toDate(self, unixdate):
        date = dt.datetime.fromtimestamp(int(unixdate))
        return date.strftime('%d/%m/%Y %H:%M:%S')


def main(host='localhost', port=8086):
    cache = PoloCache(host, port)
    cache.sync()


def parse_args():
    parser = argparse.ArgumentParser(
        description='InfluxDB Poloniex OHLC Cache')
    parser.add_argument('--host', type=str, required=False,
                        default='localhost',
                        help='hostname of InfluxDB http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port of InfluxDB http API')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port)
