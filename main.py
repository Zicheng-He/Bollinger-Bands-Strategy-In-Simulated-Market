# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 23:16:42 2020

@author: 63438
"""

import json
import datetime as dt
from dateutil import tz
from dateutil.relativedelta import relativedelta
import urllib.request
import pandas as pd
import numpy as np
from sqlalchemy import Column, ForeignKey, Integer, Float, String
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect
import ClientFrontEnd
from BBD_client import Client_Manual



engine = create_engine('sqlite:///TradingBBD.db',connect_args={'check_same_thread': False})
conn = engine.connect()
conn.execute("PRAGMA foreign_keys = ON")

metadata = MetaData()
metadata.reflect(bind=engine)


end_date = dt.datetime.today().date()
start_date = end_date - relativedelta(weeks=1)

### This week we dont do FrontEnd but Manual connect
FrontEnd = True
Manual_Client_Connect = False

if __name__ == "__main__":
    if FrontEnd:
        app = ClientFrontEnd.FrontEnd(engine)
        app.run()
    if Manual_Client_Connect:
        Client_Manual()
