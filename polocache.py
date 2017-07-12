# -*- coding: utf-8 -*-

from influxdb import DataFrameClient
from dateutil.relativedelta import relativedelta

import argparse
import requests
import logging
import time

import datetime as dt
import pandas as pd


class PoloCache(object):
    """
    Poloniex OHLC cache stored in InfluxDB.
    """
    BASE_URL = 'https://poloniex.com/public?command='
    PERIODS = [300, 900, 1800, 7200, 14400, 86400]

    DF_FIELDS = ['date', 'open', 'low', 'high', 'close',
                 'weightedAverage', 'volume', 'quoteVolume']

    def __init__(self):
        """
        Get list of supported assets.
        """
        self.assets = self._get_assets()
        assert len(self.assets) > 0

    def _get_assets(self):
        """
        Return a dictionary
        :return:
        :rtype: tuple
        """
        assets = requests.get('%sreturnCurrencies' % PoloCache.BASE_URL).json()
        return assets

    def sync(self, disabled=False, delisted=False, frozen=False):
        """
        Sync poloniex db to a local InfluxDB instance.

        :param disabled: sync disabled assets. Default: False
        :type disabled: bool
        :param delisted: sync delisted assets. Default: False
        :type delisted: bool
        :param frozen: sync frozen assets. Default: False
        :type frozen: bool

        :return: number of candles inserted to InfluxDB instance
        :rtype: int

        :raises py::class:SyncError if sync failed.
        """
        import pprint
        for asset in self.assets:

            sync_disabled = bool(self.assets[asset]['disabled']) and disabled
            sync_delisted = bool(self.assets[asset]['delisted']) and delisted
            sync_frozen = bool(self.assets[asset]['frozen']) and frozen
            do_sync = sync_delisted or sync_disabled or sync_frozen
            if not do_sync:
                continue
            else:
                pprint.pprint(asset)





    def _sync_asset(self, asset):
        pass

    def toUnix(self, stringdate):
        #date = dt.datetime.strptime(stringdate, "%d/%m/%Y")
        return int(time.mktime(stringdate.timetuple()))

    def toDate(self, unixdate):
        date = dt.datetime.fromtimestamp(int(unixdate))
        return date.strftime('%d/%m/%Y %H:%M:%S')

    def buildURL(self, pair, period, start, end):
        # base =
        return '%sBTC_AMP&start=%s&end=%s&period=%s' % \
               (base, toUnix(start), toUnix(end), period)


class TimeSeries(object):

    def __init__(self):
        self.empty = True
        self.data = None
        self.pair = ("None", "None")
        self.period = "None"
        self.start = "None"
        self.end = "None"

    def getData(self, pair, period, start, end):
        url = buildURL(pair, period, start, end)
        datapoints = requests.get(url).json()

        print(url)

        data = []
        for dtp in datapoints:
            row = []
            for fld in fields:
                if fld == 'date':
                    row.append(toDate(dtp[fld]))
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


def main(host='localhost', port=8086):
    """Instantiate the connection to the InfluxDB client."""
    user = 'root'
    password = 'root'
    db_name = 'BTC_AMP_900'
    protocol = 'json'

    cache = PoloCache()
    cache.sync(disabled=True)
    return

    client = DataFrameClient(host, port, user, password, db_name)

    dbs = [db['name'] for db in client.get_list_database()]
    if db_name not in dbs:
        print("Create database: " + db_name)
        client.create_database(db_name)

    t = TimeSeries()
    polo_start = dt.datetime.strptime("01/01/2014", "%d/%m/%Y")

    start_month = polo_start
    end_month = polo_start + relativedelta(months=1)
    while end_month <= dt.datetime.now():

        t.getData("BTC_AMP", 900, start_month, end_month)
        start_month = end_month
        end_month = end_month + relativedelta(months=1)

        #pprint.pprint(len(t.data))
        #print("Writing %s to %s." % (start_month, end_month))
        if len(t.data) > 1:
            client.write_points(t.data, db_name, protocol=protocol)

        q = client.query("select * from %s where time" % db_name)

        print(q)
        print(len(q))

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
