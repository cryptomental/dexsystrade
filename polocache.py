# -*- coding: utf-8 -*-

from influxdb import DataFrameClient
from dateutil.relativedelta import relativedelta

import argparse
import requests
import logging
import time

import datetime as dt
import pandas as pd


class PoloCacheError(Exception):
    pass


class PoloCache(object):
    """
    Poloniex OHLC cache stored in InfluxDB.
    """
    BASE_URL = 'https://poloniex.com/public?command='
    PERIODS = [300, 900, 1800, 7200, 14400, 86400]

    def __init__(self):
        """
        Get list of supported assets.
        """
        self.assets = PoloCache.__get_assets()
        assert len(self.assets) > 0

    @staticmethod
    def __get_assets():
        """
        Return a dictionary
        :return:
        :rtype: tuple
        """
        return requests.get('%sreturnCurrencies' % PoloCache.BASE_URL).json()

    def __can_sync(self, asset, sync_disabled, sync_delisted, sync_frozen):
        """
        Decide whether to sync an asset or not.

        :param asset: asset to sync
        :param sync_disabled:
        :param sync_delisted:
        :param sync_frozen:

        :return: True if to sync, False otherwise
        """
        if bool(self.assets[asset]['disabled']) and not sync_disabled:
            return False
        if bool(self.assets[asset]['delisted']) and not sync_delisted:
            return False
        if bool(self.assets[asset]['frozen']) and not sync_frozen:
            return False

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

    def __sync(self, asset):
        """
        Sync selected asset to InfluxDb.

        :param asset: asset name
        :return:
        """
        host = 'localhost'
        port = 8086
        user = 'root'
        password = 'root'
        db_name = 'poloniex'
        protocol = 'json'

        client = DataFrameClient(host, port, user, password, db_name)
        PoloCache.__create_db_if_does_not_exist(client, db_name)

        for period in PoloCache.PERIODS:
            t = TimeSeries()
            polo_start = dt.datetime.strptime("01/01/2014", "%d/%m/%Y")

            start_month = polo_start
            end_month = polo_start + relativedelta(months=1)

            #print("Expecting %s OHLC candles for %s period." %
            #      (str((end_month-start_month)/period), period))

            while end_month <= dt.datetime.now():

                t.getData("%s" % asset, period, start_month, end_month)
                start_month = end_month
                end_month = end_month + relativedelta(months=1)

                print(len(t.data))
                print("Writing %s to %s." % (start_month, end_month))

                if len(t.data) > 1:
                    client.write_points(t.data, db_name, protocol=protocol)

        q = client.query("select * from %s where time" % db_name)
        print(q)

    def sync(self, sync_disabled=False, sync_delisted=False, sync_frozen=False):
        """
        Sync poloniex db to a local InfluxDB instance.

        :param sync_disabled: sync disabled assets. Default: False
        :type sync_disabled: bool
        :param sync_delisted: sync delisted assets. Default: False
        :type sync_delisted: bool
        :param sync_frozen: sync frozen assets. Default: False
        :type sync_frozen: bool

        :return: number of candles inserted to InfluxDB instance
        :rtype: int

        :raises py::class:SyncError if sync failed.
        """
        import pprint
        for asset in self.assets:
            if self.__can_sync(asset, sync_disabled, sync_delisted, sync_frozen):
                print asset
                self.__sync(asset)


class TimeSeries(object):

    def __init__(self):
        self.empty = True
        self.data = None
        self.pair = ("None", "None")
        self.period = "None"
        self.start = "None"
        self.end = "None"

    def getData(self, asset, period, start, end):
        fields = ['date', 'open', 'low', 'high', 'close',
                  'weightedAverage', 'volume', 'quoteVolume']

        url = self.build_url(asset, period, start, end)
        datapoints = requests.get(url).json()

        print(url)

        data = []
        for dtp in datapoints:
            row = []
            for fld in fields:
                if fld == 'date':
                    print(dtp[fld])
                    row.append(self.toDate(dtp[fld]))
                else:
                    row.append(dtp[fld])
            data.append(row)

        temp_df = pd.DataFrame(data, columns=fields)
        temp_df.index = pd.to_datetime(temp_df['date'], dayfirst=True)
        temp_df.drop('date', axis=1, inplace=True)

        self.data = temp_df
        self.empty = False
        self.pair = asset
        self.period = period
        self.start = start
        self.end = end

    def build_url(self, asset, period, start, end):
        return '%sreturnChartData&currencyPair=BTC_%s&start=%s&end=%s&period=%s' % \
               (PoloCache.BASE_URL, asset, self.toUnix(start), self.toUnix(end), period)

    def toUnix(self, stringdate):
        #date = dt.datetime.strptime(stringdate, "%d/%m/%Y")
        return int(time.mktime(stringdate.timetuple()))

    def toDate(self, unixdate):
        date = dt.datetime.fromtimestamp(int(unixdate))
        return date.strftime('%d/%m/%Y %H:%M:%S')

def main(host='localhost', port=8086):
    """Instantiate the connection to the InfluxDB client."""
    user = 'root'
    password = 'root'
    db_name = 'poloniex'
    protocol = 'json'

    cache = PoloCache()
    cache.sync()
    return


    print("Delete database: " + db_name)
    # client.drop_database(db_name)


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
