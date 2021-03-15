# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 20:11:29 2020

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



engine = create_engine('sqlite:///TradingBBD.db',connect_args={'check_same_thread': False})
conn = engine.connect()
conn.execute("PRAGMA foreign_keys = ON")


metadata = MetaData()
metadata.reflect(bind=engine)


end_date = dt.datetime.today().date()
start_date = end_date - relativedelta(weeks=1)


def EDTtoUnixTime(EDTdatetime):
    utcTime = EDTdatetime.replace(tzinfo = tz.gettz('EDT')).astimezone(tz=dt.timezone.utc)
    unixTime = utcTime.timestamp()
    return str(int(unixTime))
    
def get_sp500_component():
    url = 'https://eodhistoricaldata.com/api/fundamentals/GSPC.INDX?api_token=5ba84ea974ab42.45160048'
    with urllib.request.urlopen(url) as req:
        data = json.load(req)
        comdf = pd.DataFrame(data['Components']).T
        tickerList = list(comdf['Code'])
    return tickerList[:30] ### for demo, otherwise it is time consuming


def get_daily_data(symbol, 
                   start=start_date, 
                   end=end_date, 
                   requestURL='https://eodhistoricaldata.com/api/eod/', 
                   apiKey='5ba84ea974ab42.45160048'):
    symbolURL = str(symbol) + '.US?'
    startURL = 'from=' + str(start)
    endURL = 'to=' + str(end)
    apiKeyURL = 'api_token=' + apiKey
    completeURL = requestURL + symbolURL + startURL + '&' + endURL + '&' + apiKeyURL + '&period=d&fmt=json'
    print('Possessing daily data of stock:' + symbol)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data

def get_intraday_data(symbol, 
                      interval='5m',
                      requestURL='https://eodhistoricaldata.com/api/intraday/', 
                      apiKey='5ba84ea974ab42.45160048',
                      start=None): ### start in the format of EDT datetime 
    symbolURL = str(symbol) + '.US?'
    apiKeyURL = 'api_token=' + apiKey
    if start != None:
        starttimestamp = EDTtoUnixTime(start)
        completeURL = requestURL + symbolURL + apiKeyURL + '&interval=' + interval + '&fmt=json' + '&from='+starttimestamp
    else:
        completeURL = requestURL + symbolURL + apiKeyURL + '&interval=' + interval + '&fmt=json'
    print('Possessing data of stock:' + symbol)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data
    
def create_stocks_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('Ticker', String(50), primary_key=True, nullable=False),
                  Column('H', Integer, nullable=False),
                  Column('K1', Float, nullable=False),
                  Column('Notional', Float, nullable=False),
                  Column('Profit_Loss_in_Training', Float, nullable=False),
                  Column('Return_in_Training', Float, nullable=False),
                  Column('Profit_Loss', Float, nullable=False),
                  Column('Return', Float, nullable=False),
                  extend_existing=True)
    table.create(engine)
    
                       
def create_price_table(table_name, metadata, engine):
    tables = metadata.tables.keys()
    if table_name not in tables:
        foreign_key = 'Stocks.Ticker'
        table = Table(table_name, metadata, 
                      Column('Symbol', String(50), ForeignKey(foreign_key), primary_key=True, nullable=False),
                      Column('DateTime', String(50), primary_key=True, nullable=False),
                      Column('Open', Float, nullable=False),
                      Column('High', Float, nullable=False),
                      Column('Low', Float, nullable=False),
                      Column('Close', Float, nullable=False),
                      Column('Volume', Integer, nullable=False))
        table.create(engine)
        
def create_strategy_table(table_name, metadata, engine):
    table = Table(table_name, metadata,    
                  Column('Strategy', String(50), primary_key=True, nullable=False),
                  Column('Notional', Float, nullable=False),
                  Column('PnL_in_Training', Float, nullable=False),
                  Column('Return_in_Training', Float, nullable=False),
                  Column('SPY_Return_in_Training', Float, nullable=False),
                  Column('PnL', Float, nullable=False),
                  Column('Strategy_Return', Float, nullable=False),
                  Column('SPY_Return', Float, nullable=False),
                  extend_existing=True)
    table.create(engine)

def clear_a_table(table_name, metadata, engine):
    conn = engine.connect()
    table = metadata.tables[table_name]
    delete_st = table.delete()
    conn.execute(delete_st)


def execute_sql_statement(sql_stmt, engine, change=False):
        if change:
            engine.execute(sql_stmt)
        else:
            result_set = engine.execute(sql_stmt)
            result_df = pd.DataFrame(result_set.fetchall())
            if not result_df.empty:
                result_df.columns = result_set.keys()
            return result_df
        
def stock_selection(stock_list, interval='5m', number_of_stocks=10):
    std_resultdf = pd.DataFrame(index=stock_list)
    std_resultdf['std'] = 0.0
    for stk in stock_list:
        try:
            stk_data = pd.DataFrame(get_intraday_data(stk, interval))
            std = stk_data.close.pct_change().shift(-1).std()
            std_resultdf.loc[stk,'std'] = std
            print('Volatility of return over stock: ' + stk + ' is: ' + str(std))
        except:
            print('Cannot get data of Stock:' + stk)
    stock_selected = list(std_resultdf['std'].sort_values().index[-number_of_stocks:])
    selected_df = std_resultdf['std'].sort_values()[-number_of_stocks:]
    return stock_selected, selected_df


def populate_price_data(stockList, engine, table_name, start_date, end_date, interval='1m'):
    column_names = ['Symbol', 'DateTime', 'Open', 'High', 'Low', 'Close', 'Volume']
    price_data = []
    mkt_opentime = dt.datetime.strptime('09:30','%H:%M').time()
    mkt_closetime = dt.datetime.strptime('16:00','%H:%M').time()
    start_datetime = dt.datetime(start_date.year,start_date.month,start_date.day,9,30)
    for stk in stockList:
        raw_data = get_intraday_data(stk,interval,start=start_datetime)
        for stock_data in raw_data:
            price_data.append([stk, stock_data['datetime'], stock_data['open'], stock_data['high'], stock_data['low'], \
                               stock_data['close'], stock_data['volume']])
        print(stk + '\'s data has been stored.')
        
    price = pd.DataFrame(price_data, columns=column_names)
    ### Convert UTC to EST
    price.DateTime = pd.to_datetime(price.DateTime) - dt.timedelta(hours=5)
    ### Select during Trading Hour and within selected period
    price = price[(price.DateTime.dt.time>=mkt_opentime) & (price.DateTime.dt.time<=mkt_closetime)]
    price = price[(price.DateTime.dt.date>=start_date) & (price.DateTime.dt.date<=end_date)]
    price.to_sql(table_name, con=engine, if_exists='append', index=False)

def GridSearchinDBBD(stkdata,H,K1):
    data = stkdata.copy()
    Notional = 1000000.00 / 10
    data['SMA'] = data['close'].rolling(H).mean()
    data['rstd'] = data['close'].rolling(H).std()
    data['Up1'] = data['SMA'] + K1 * data['rstd']
    data['Down1'] = data['SMA'] - K1 * data['rstd']
    
    ### signals
    data['signal'] = 0
    data.loc[data['close'] >= data['Up1'],'signal'] = -1
    data.loc[(data['close'] < data['Up1']) & (data['close'] > data['Down1']),'signal'] = 0
    data.loc[data['close'] <= data['Down1'],'signal'] = 1
    data.signal = data.signal.shift().fillna(0)
    data['trade'] = data.signal.diff()
    
    ### PnL cal
    data['pre_trade_pos'] = 0
    data['target_pos'] = 0
    data['realized_pnl_d'] = 0
    last_target_pos = 0
    for index, row in data.iterrows():
        data.loc[index,'pre_trade_pos'] = last_target_pos
        if row.trade != 0:
            data.loc[index,'target_pos'] = row.signal * int(Notional / row['close'])
            last_target_pos = row.signal * int(Notional / row['close'])
        
        if abs(row.signal) < abs(row.trade):
            if row.trade < 0:
                data.loc[index,'realized_pnl_d'] = data.loc[index,'pre_trade_pos'] * row['close'] - Notional 
            else:
                data.loc[index,'realized_pnl_d'] = Notional + data.loc[index,'pre_trade_pos'] * row['close']
    
    data['realized_pnl_p'] = data['realized_pnl_d'] / Notional
    data['cum_pnl_p'] = data['realized_pnl_p'].cumsum() + 1
    data['cum_pnl_p_diff'] = data['cum_pnl_p'].diff()
    
    ### IR calculation
    data.index = data.datetime
    daily_return = pd.DataFrame(data.realized_pnl_p).copy()
    daily_return['date'] = daily_return.index
    daily_return['date'] = daily_return['date'].apply(lambda x:x.date())
    df_whole = daily_return.groupby(['date']).sum()
    # performance strat
    cumulative_return = df_whole.cumsum()
    Annualized_return = cumulative_return.iloc[-1,:]* 252 / len(df_whole)
    Annualized_vol = df_whole.std() * np.sqrt(252)
    Information_ratio = (Annualized_return / Annualized_vol)[0]
    CumPnL = cumulative_return.iloc[-1,:][0] * Notional
    return Information_ratio, CumPnL

    

def train_params_DBBD(stk_list,H_list,K1_list,train_end_date,period='1M'):
    if period[1] == 'M':
        train_start_date = train_end_date - relativedelta(months=int(period[0]))
    elif period[1] == 'W':
        train_start_date = train_end_date - relativedelta(weeks=int(period[0]))
    train_start = dt.datetime(train_start_date.year,train_start_date.month,train_start_date.day,9,30)
    mkt_opentime = dt.datetime.strptime('09:30','%H:%M').time()
    mkt_closetime = dt.datetime.strptime('16:00','%H:%M').time()
    stocks = pd.DataFrame(stk_list,columns=['Ticker'])
    stocks["H"] = 0
    stocks["K1"] = 0.0
    stocks['Notional'] = 1000000.00 / 10
    stocks["Profit_Loss_in_Training"] = 0.0
    stocks['Return_in_Training'] = 0.0
    stocks["Profit_Loss"] = 0.0
    stocks['Return'] = 0.0
    for stk in stk_list:
        print("Training params for: " + stk +' ...')
        train_data = pd.DataFrame(get_intraday_data(stk, interval='1m',start=train_start))
        ### Convert UTC to EST
        train_data.datetime = pd.to_datetime(train_data.datetime) - dt.timedelta(hours=5)
        ### Select during Trading Hour and within selected period
        train_data = train_data[(train_data.datetime.dt.time>=mkt_opentime) & (train_data.datetime.dt.time<=mkt_closetime)]
        train_data = train_data[(train_data.datetime.dt.date>=train_start_date) & (train_data.datetime.dt.date<train_end_date)]
        IR_df = pd.DataFrame(index=H_list,columns=K1_list)
        CumPnLdf = pd.DataFrame(index=H_list,columns=K1_list)
        try:
            for H in H_list:
                for K1 in K1_list:
                    IR, CumPnL = GridSearchinDBBD(train_data,H,K1)
                    IR_df.loc[H,K1] = IR
                    CumPnLdf.loc[H,K1] = CumPnL
                    print(stk + ':H,K pair:(' + str(H) + ',' + str(K1) + ')done, with CumPnL:' + str(CumPnL))
            ### select the pair from IR
            H0 = CumPnLdf.mean(axis=1).idxmax()
            K10 = CumPnLdf.mean().idxmax()
            ### delete those with negative PnL within training period
            if CumPnLdf.loc[H0,K10] <= 0:
                print('Training performance bad, delete stk:{}.'.format(stk))
                stocks = stocks.drop(stocks[stocks.Ticker==stk].index)
            else:
                stocks.loc[stocks[stocks.Ticker==stk].index,'H'] = H0
                stocks.loc[stocks[stocks.Ticker==stk].index,'K1'] = K10
                stocks.loc[stocks[stocks.Ticker==stk].index,'Profit_Loss_in_Training'] = CumPnLdf.loc[H0,K10]
                stocks.loc[stocks[stocks.Ticker==stk].index,'Return_in_Training'] = CumPnLdf.loc[H0,K10] * 10 / 1000000.00
        except:
            print("Deleted. Missing data for stk: " + stk)
            stocks = stocks.drop(stocks[stocks.Ticker==stk].index)
    return stocks
        
def stk_select():
    sp500_symbol_list = get_sp500_component()
    selected_stk, stk_df = stock_selection(sp500_symbol_list)
    return selected_stk, stk_df
        
def build_trading_model(stk_list):
    engine.execute('Drop Table if exists Stocks;')	
    create_stocks_table('Stocks', metadata, engine)
    H_list = [40,50,60,70,80,90]
    K1_list = [1.5,1.8,2.0,2.2,2.5]
    stocks = train_params_DBBD(stk_list,H_list,K1_list,train_end_date=start_date,period='1W')
    
    stocks.to_sql('Stocks', con=engine, if_exists='append', index=False)
    
    return stocks
    
def populate_backtest_data(stocks):
    ### CREATE price table
    create_price_table('Price', metadata, engine)
    inspector = inspect(engine)
    print(inspector.get_table_names())
    clear_a_table('Price', metadata, engine)
	### populate price table
    populate_price_data(stocks['Ticker'].unique(), engine, 'Price', start_date, end_date)
    
    ### create strategy table
    engine.execute('Drop Table if exists Strategy;')	
    create_strategy_table('Strategy', metadata, engine)
  
def backTest():
    ### read data through data base
    stockdf = execute_sql_statement("SELECT * from Stocks",conn)
    pricedf = execute_sql_statement("SELECT * from Price", conn)
    ### divide the notional
    Notional = 1000000 // len(stockdf['Ticker'].unique())
    for stk in stockdf['Ticker'].unique():
        print("Back Testing:" + stk)
        data = pricedf[pricedf.Symbol==stk].copy()
        ### select the right parameters according to the training result
        H = stockdf.loc[stockdf[stockdf.Ticker==stk].index,'H'].iloc[0]
        K1 = stockdf.loc[stockdf[stockdf.Ticker==stk].index,'K1'].iloc[0]
        ## calculating rolling H-period Moving average and rolling std
        ## the number of the std to form the band is K1 
        data['SMA'] = data['Close'].rolling(H).mean()
        data['rstd'] = data['Close'].rolling(H).std()
        data['Up1'] = data['SMA'] + K1 * data['rstd']
        data['Down1'] = data['SMA'] - K1 * data['rstd']
        
        ### signals generation
        data['signal'] = 0
        ## when close larger than ma + k1 * std ->　short, when close smaller than ma - k1 * std -> long
        data.loc[data['Close'] >= data['Up1'],'signal'] = -1
        data.loc[(data['Close'] < data['Up1']) & (data['Close'] > data['Down1']),'signal'] = 0
        data.loc[data['Close'] <= data['Down1'],'signal'] = 1
        ##　shift signal since we trade at next min
        data.signal = data.signal.shift().fillna(0)
        data['trade'] = data.signal.diff()
        
        ### PnL calculation
        data['pre_trade_pos'] = 0
        data['target_pos'] = 0
        data['realized_pnl_d'] = 0
        last_target_pos = 0 # store the target position we trade at last entry, in order to cal the realized pnl
        for index, row in data.iterrows():
            data.loc[index,'pre_trade_pos'] = last_target_pos
            if row.trade != 0:
                ## cal the position according to Notional and close, cannot do fraction so always make it int
                data.loc[index,'target_pos'] = row.signal * int(Notional / row['Close'])
                last_target_pos = row.signal * int(Notional / row['Close'])
            
            if abs(row.signal) < abs(row.trade): ## when it is an exit, we calculate the realized pnl of this trade
                if row.trade < 0:
                    data.loc[index,'realized_pnl_d'] = data.loc[index,'pre_trade_pos'] * row['Close'] - Notional 
                else:
                    data.loc[index,'realized_pnl_d'] = Notional + data.loc[index,'pre_trade_pos'] * row['Close']
        ## cal pnl in percentage, also cumulative pnl
        data['realized_pnl_p'] = data['realized_pnl_d'] / Notional
        data['cum_pnl_p'] = data['realized_pnl_p'].cumsum() 
        
        CumPnL = data['realized_pnl_d'].cumsum().iloc[-1]
        Return = data['cum_pnl_p'].iloc[-1]
        engine.execute("UPDATE Stocks SET Profit_Loss = " + str(CumPnL) + " WHERE Ticker = '" + stk +"'")	
        engine.execute("UPDATE Stocks SET Notional = " + str(Notional) + " WHERE Ticker = '" + stk +"'")	
        engine.execute("UPDATE Stocks SET Return = " + str(Return) + " WHERE Ticker = '" + stk +"'")

def probation_test(probation_testing_start_date, probation_testing_end_date):
    ### read data through data base
    stockdf = pd.read_sql_query("SELECT * from Stocks", conn)
    pricedf = pd.read_sql_query("SELECT * from Price", conn)
    #engine.execute('Drop Table if exists Probation;')	
    #create_stocks_table('Probation', metadata, engine)
    stocks = stockdf.copy(deep=True)
    stocks["Profit_Loss_in_Training"] = 0.0
    stocks['Return_in_Training'] = 0.0
    stocks["Profit_Loss"] = 0.0
    stocks['Return'] = 0.0
    ### divide the notional
    Notional = 1000000 // len(stockdf['Ticker'].unique())
    for stk in stockdf['Ticker'].unique():
        print("Back Testing:" + stk)
        data = pricedf[pricedf.Symbol==stk].copy()
        data = data[(data.DateTime>=probation_testing_start_date)&(data.DateTime<=probation_testing_end_date)]
        ### select the right parameters according to the training result
        H = stockdf.loc[stockdf[stockdf.Ticker==stk].index,'H'].iloc[0]
        K1 = stockdf.loc[stockdf[stockdf.Ticker==stk].index,'K1'].iloc[0]
        ## calculating rolling H-period Moving average and rolling std
        ## the number of the std to form the band is K1 
        data['SMA'] = data['Close'].rolling(H).mean()
        data['rstd'] = data['Close'].rolling(H).std()
        data['Up1'] = data['SMA'] + K1 * data['rstd']
        data['Down1'] = data['SMA'] - K1 * data['rstd']
        
        ### signals generation
        data['signal'] = 0
        ## when close larger than ma + k1 * std ->　short, when close smaller than ma - k1 * std -> long
        data.loc[data['Close'] >= data['Up1'],'signal'] = -1
        data.loc[(data['Close'] < data['Up1']) & (data['Close'] > data['Down1']),'signal'] = 0
        data.loc[data['Close'] <= data['Down1'],'signal'] = 1
        ##　shift signal since we trade at next min
        data.signal = data.signal.shift().fillna(0)
        data['trade'] = data.signal.diff()
        
        ### PnL calculation
        data['pre_trade_pos'] = 0
        data['target_pos'] = 0
        data['realized_pnl_d'] = 0
        last_target_pos = 0 # store the target position we trade at last entry, in order to cal the realized pnl
        for index, row in data.iterrows():
            data.loc[index,'pre_trade_pos'] = last_target_pos
            if row.trade != 0:
                ## cal the position according to Notional and close, cannot do fraction so always make it int
                data.loc[index,'target_pos'] = row.signal * int(Notional / row['Close'])
                last_target_pos = row.signal * int(Notional / row['Close'])
            
            if abs(row.signal) < abs(row.trade): ## when it is an exit, we calculate the realized pnl of this trade
                if row.trade < 0:
                    data.loc[index,'realized_pnl_d'] = data.loc[index,'pre_trade_pos'] * row['Close'] - Notional 
                else:
                    data.loc[index,'realized_pnl_d'] = Notional + data.loc[index,'pre_trade_pos'] * row['Close']
        ## cal pnl in percentage, also cumulative pnl
        data['realized_pnl_p'] = data['realized_pnl_d'] / Notional
        data['cum_pnl_p'] = data['realized_pnl_p'].cumsum() 
        
        CumPnL = data['realized_pnl_d'].cumsum().iloc[-1]
        stocks.loc[stocks[stocks.Ticker==stk].index,'Profit_Loss'] = CumPnL
        #stocks.to_sql('Probation', con=engine, if_exists='append', index=False)
    return stocks

def populate_strategy_bt():
    engine.execute('Drop Table if exists Strategy;')	
    stockdf = pd.read_sql_query("SELECT * from Stocks", conn)
    Notional = 1000000.00
    train_Pnl = stockdf.Profit_Loss_in_Training.sum()
    train_Ret = train_Pnl / Notional
    
    bt_Pnl =  stockdf.Profit_Loss.sum()
    bt_Ret = bt_Pnl / Notional
    
    ### divide time into training period and bt period
    bt_end_date = dt.datetime.today().date()
    bt_start_date = end_date - relativedelta(weeks=1)
    train_end_date = bt_start_date - relativedelta(days=1)
    train_start_date = train_end_date - relativedelta(weeks=1)
    ### get SPY data
    spy_df = pd.DataFrame(get_daily_data('SPY',train_start_date,bt_end_date))
    train_start_open = spy_df.open.iloc[0]
    train_end_close = spy_df.loc[spy_df.date<=str(train_end_date),'close'].iloc[-1]
    
    ### calculate return of SPY, dividing into training and back testing
    bt_start_open = spy_df.loc[spy_df.date>=str(bt_start_date),'open'].iloc[0]
    bt_end_close = spy_df.close.iloc[-1]
    train_ret = (train_end_close - train_start_open) / train_start_open
    bt_ret = (bt_end_close - bt_start_open) / bt_start_open
    
    ### populate result into strategy table
    strategy = pd.DataFrame(index=[0],columns = ['Strategy'])
    strategy['Strategy'] = 'BB'
    strategy['Notional'] = Notional
    strategy['PnL_in_Training'] = train_Pnl
    strategy['Return_in_Training'] = train_Ret
    strategy['SPY_Return_in_Training'] = train_ret
    strategy['PnL'] = bt_Pnl
    strategy['Strategy_Return'] = bt_Ret
    strategy['SPY_Return'] = bt_ret
    
    strategy.to_sql('Strategy', con=engine, if_exists='append', index=False)


