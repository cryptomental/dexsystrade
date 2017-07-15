from influxdb import DataFrameClient
from dateutil.relativedelta import relativedelta
from talib.abstract import *

import argparse
import requests
import logging
import time

import datetime as dt
import pandas as pd

from polocache import PoloCache

import pprint


class TradingRuleError(Exception):
    pass


class TradingRule(object):
    def __init__(self, host, port):
        """
        Trading rule test
        """
        self.host = host
        self.port = port

    def check(self, pair, period):
        user = 'root'
        password = 'root'
        db_name = 'poloniex'
        protocol = 'json'

        client = DataFrameClient(self.host, self.port, user, password, db_name)

        influxdb_series = "%s_%s" % (pair, period)

        end_month = dt.datetime.now()
        start_month = end_month - relativedelta(months=1)
        period = 300

        q = client.query("select * from %s where time >= '%s' and time < '%s';"
                         % (influxdb_series, start_month, end_month))
        if q:
            print("Available %d candles for %s in InfluxDB for period %d" %
                  (len(q[influxdb_series].index), pair, period))

        n1 = 10
        n2 = 21

        """
        ap = hlc3
        esa = ema(ap, n1)
        d = ema(abs(ap - esa), n1)
        ci = (ap - esa) / (0.015 * d)
        tci = ema(ci, n2)

        wt1 = tci
        wt2 = sma(wt1, 4)
        """

        input = q[influxdb_series]

        ap = TYPPRICE(input)
        ap = pd.DataFrame(ap)
        ap.columns = ['close']

        esa = EMA(ap, timeperiod=n1)
        esa = pd.DataFrame(esa)
        esa.columns = ['close']

        d = EMA(abs(ap - esa), timeperiod=n1)
        d = pd.DataFrame(d)
        d.columns = ['close']

        ci = (ap - esa) / (0.015 * d)

        tci = EMA(ci, timeperiod=n2)
        tci = pd.DataFrame(tci)
        tci.columns = ['close']

        wt1 = tci
        wt2 = SMA(wt1, 4)

        oversold1 = -53
        oversold2 = -60

        for idx in range(len(wt1.values)-25, len(wt1.values)-1):
            zielony = wt1.values[idx][0]
            zielony_prev = wt1.values[idx-1][0]
            czerwony = wt2.values[idx]
            czerwony_prev = wt2.values[idx-1]

            # pprint.pprint(zielony)      # zielony
            # pprint.pprint(czerwony)      # czerwony
            if zielony > czerwony and zielony_prev < czerwony_prev:
                print pair, "zielona kropka", input.index[idx], zielony
                if zielony < oversold1:
                    print "BUY", zielony
            if zielony < czerwony and zielony_prev > czerwony_prev:
                print pair, "czerwona kropka", input.index[idx], zielony


if __name__ == '__main__':
    t = TradingRule('localhost', 8086)
    cache = PoloCache('localhost', 8086)
    for pair in cache.pairs:
        t.check(pair, 14400)
