# -*- coding: utf-8 -*-
#!/usr/bin/env python3
#@ Copyright - Song Tang

import json
import urllib.request
import sys
import pandas as pd
import random
import numpy as np

from socket import AF_INET, socket, SOCK_STREAM, IPPROTO_TCP, TCP_NODELAY, gethostname, gethostbyname
import threading
import queue

from sqlalchemy import create_engine
from sqlalchemy import MetaData

import sched, time
import datetime

from pandas.tseries.holiday import USFederalHolidayCalendar, GoodFriday
from pandas.tseries.offsets import CustomBusinessDay

from network import PacketTypes, Packet

import pandas_market_calendars as mcal

serverID = "Server"
engine = create_engine('sqlite:///BBDTradingServer.db')
conn = engine.connect()
conn.execute("PRAGMA foreign_keys = ON")

metadata = MetaData()
metadata.reflect(bind=engine)

mutex = threading.Lock()

requestURL = "https://eodhistoricaldata.com/api/eod/"
myEodKey = "5ba84ea974ab42.45160048"
defaultStartDate = "2020-06-01"
defaultEndDate = "2020-09-30"
def get_daily_data(symbol, startDate=defaultStartDate, endDate=defaultEndDate, apiKey=myEodKey):
    symbolURL = str(symbol) + ".US?"
    startDateURL = "from=" + str(startDate)
    endDateURL = "to=" + str(endDate)
    apiKeyURL = "api_token=" + apiKey
    completeURL = requestURL + symbolURL + startDateURL + '&' + endDateURL + '&' + apiKeyURL + '&period=d&fmt=json'
    print(completeURL)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data  
    
def populate_stock_data(tickers, engine, table_name, stock_market_periods):
    column_names = ['symbol', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']
    price_data = []
    for ticker in tickers:
        stock = get_daily_data(ticker, stock_market_periods[ticker][0], stock_market_periods[ticker][len(stock_market_periods[ticker])-1])
        for stock_data in stock:
            price_data.append([ticker, stock_data['date'], stock_data['open'], stock_data['high'], stock_data['low'], \
                               stock_data['close'], stock_data['adjusted_close'], stock_data['volume']])
        print(price_data)
    stocks = pd.DataFrame(price_data, columns=column_names)
    stocks.to_sql(table_name, con=engine, if_exists='replace', index=False)

 
intradayRequestURL = "https://eodhistoricaldata.com/api/intraday/"
myEodKey = "5ba84ea974ab42.45160048"
defaultStartSeconds = "1585800000"
defaultEndSeconds   = "1585886400"
def get_intraday_data(symbol, startTime=defaultStartSeconds, endTime=defaultEndSeconds, apiKey=myEodKey):
    symbolURL = str(symbol) + ".US?"
    startDateURL = "from=" + str(startTime)
    endDateURL = "to=" + str(endTime)
    apiKeyURL = "api_token=" + apiKey
    completeURL = intradayRequestURL + symbolURL + startDateURL + '&' + endDateURL + '&' + apiKeyURL + '&period=d&fmt=json'
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data  

def populate_intraday_stock_data(tickers, engine, days_in_seconds):
    column_names = ['datetime', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    for ticker in tickers:
        stock = get_intraday_data(ticker, days_in_seconds[0], days_in_seconds[len(days_in_seconds)-1])
        print(stock)
        price_data = []
        for stock_data in stock:
            if ((stock_data['open'] is not None and stock_data['open'] > 0) and 
                (stock_data['high'] is not None and stock_data['high'] > 0) and 
                (stock_data['low']  is not None and stock_data['low'] > 0) and 
                (stock_data['close'] is not None and stock_data['close'] > 0 )and 
                (stock_data['volume'] is not None and stock_data['volume'] > 0)):
                price_data.append([stock_data['datetime'], ticker, stock_data['open'], stock_data['high'], stock_data['low'], \
                               stock_data['close'], stock_data['volume']])

        print(price_data)
        stocks = pd.DataFrame(price_data, columns=column_names)
        stocks = stocks.dropna()
        stocks.to_sql(ticker, con=engine, if_exists='replace', index=False)

def populate_intraday_order_map(symbols, engine, market_periods):
    for i in range(len(market_periods)):
        intraday_order_map[market_periods[i]] = []
    
    stock_market_periods = {}
    for symbol in symbols:
        stock_market_periods[symbol] = []
        select_st = "SELECT * FROM " + symbol + ";"
        result_set = engine.execute(select_st)
        result_df = pd.DataFrame(result_set.fetchall())
        result_df.columns = result_set.keys()
        
        for i in range(len(market_periods)):
            if (result_df['datetime'].str.contains(market_periods[i])).any():
                mask = (result_df['datetime'].str.contains(market_periods[i])) & (result_df['symbol'] == symbol)
                result = result_df.loc[(mask.values)]
                intraday_order_map[market_periods[i]].append(result[['symbol', 'open', 'high', 'low', 'close', 'volume']])
                stock_market_periods[symbol].append(market_periods[i])
                
    #print(intraday_order_map, file = intrday_order_file) 
    
    return intraday_order_map, stock_market_periods
    
    
def accept_incoming_connections(q=None):
    while True:
        try:
            client, client_address = fre_server.accept()
            print("%s:%s has connected." % client_address, file=server_output)
            client_thread = threading.Thread(target=handle_client, args=(client,q))
            client_thread.setDaemon(True)
            client_thread.start()
        except (KeyError, KeyboardInterrupt, SystemExit, Exception):
            print("Exception in accept_incoming_connections\n", file=server_output)
            q.put(Exception("accept_incoming_connections"))
            break


def receive(client_socket):
    total_client_request = b''
    msgSize = 0
    while True:
        try:
            client_request = client_socket.recv(buf_size)
            list_client_requests = []
            if len(client_request) > 0:
                total_client_request += client_request
                msgSize = len(total_client_request)
                while msgSize > 0:
                    if msgSize > 12:
                        client_packet = Packet()
                        client_request = client_packet.deserialize(total_client_request)
                        #print(client_packet.m_msg_size, msgSize, len(client_request), file=server_output)
                    if msgSize > 12 and client_packet.m_data_size <= msgSize:
                        #data = json.loads(client_packet.m_data)
                        #print(type(data), data, file=server_output)
                        total_client_request = total_client_request[client_packet.m_data_size:]
                        msgSize = len(total_client_request)
                        client_request = b''
                        list_client_requests.append(client_packet)
                    else:
                        client_request = client_socket.recv(buf_size)
                        total_client_request += client_request
                        msgSize = len(total_client_request)
                    
            return list_client_requests
            
        except (OSError, Exception):  
            del clients[client_socket]
            print("Exception in receive\n", file=server_output)
            q.put(Exception("receive"))
            #raise Exception('receive')
            break
            
        
def handle_client(client, q=None):  
    global symbols
    global market_period
    while True:
        try:
            list_client_requests = receive(client)
            for client_request in list_client_requests:
                msg_data = json.loads(client_request.m_data)
                msg_type = client_request.m_type
                print(msg_data, file=server_output)
                clientID = msg_data["Client"]
                
                server_packet = Packet()
                
                if msg_type == PacketTypes.CONNECTION_REQ.value:
                    server_packet.m_type = PacketTypes.CONNECTION_RSP.value
                    if (clientID in clients.values()):
                        text = "%s duplicated connection request!" % clientID
                        server_msg = json.dumps({'Server': serverID, 'Response': text, 'Status': 'Rejected'})
                    else:
                        client_symbols = list(msg_data["Symbol"].split(','))
                        if all(symbol in symbols for symbol in client_symbols):
                            text = "Welcome %s!" % clientID
                            server_msg = json.dumps({'Server': serverID, 'Response': text, 'Status': 'Ack'})
                            clients[client] = clientID
                        else:
                             text = "%s Not all your symbols are eligible!" % clientID
                             server_msg = json.dumps({'Server': serverID, 'Response': text, 'Status': 'Rejected'})
                    server_packet.m_data = server_msg
                    client.send(server_packet.serialize())
                    data = json.loads(server_packet.m_data)
                    print(data, file=server_output)
                            
                elif msg_type == PacketTypes.END_REQ.value:
                    text = "%s left!" % clientID
                    server_msg = json.dumps({'Server':serverID, 'Response':text, 'Status':'Done'})
                    server_packet.m_type = PacketTypes.END_RSP.value
                    server_packet.m_data = server_msg
                    client.send(server_packet.serialize())
                    data = json.loads(server_packet.m_data)
                    print(data, file=server_output)
          
                elif msg_type == PacketTypes.CLIENT_LIST_REQ.value:
                    user_list = str('')
                    for clientKey in clients:
                        user_list += clients[clientKey] + str(',')
                        print(clients[clientKey], file=server_output)
                    server_msg = json.dumps({'Client List':user_list})
                    server_packet.m_type = PacketTypes.CLIENT_LIST_RSP.value
                    server_packet.m_data = server_msg
                    client.send(server_packet.serialize())
                    data = json.loads(server_packet.m_data)
                    print(data, file=server_output)  
                    
                elif msg_type == PacketTypes.STOCK_LIST_REQ.value:
                    stock_list = ','.join(symbols)
                    server_msg = json.dumps({"Stock List":stock_list})
                    server_packet.m_type = PacketTypes.STOCK_LIST_RSP.value
                    server_packet.m_data = server_msg
                    client.send(server_packet.serialize())
                    data = json.loads(server_packet.m_data)
                    print(data, file=server_output)  
                    
                elif msg_type == PacketTypes.SERVER_DOWN_REQ.value:
                    server_msg = json.dumps({'Server':serverID, 'Status':'Server Down Confirmed'})
                    server_packet.m_type = PacketTypes.SERVER_DOWN_RSP.value
                    server_packet.m_data = server_msg
                    client.send(server_packet.serialize())
                    data = json.loads(server_packet.m_data)
                    print(data, file=server_output)  
                    
                elif msg_type == PacketTypes.BOOK_INQUIRY_REQ.value:
                    server_packet.m_type = PacketTypes.BOOK_INQUIRY_RSP.value
                    if "Symbol" in msg_data and msg_data["Symbol"] != "":
                        if order_table.empty:
                            print("Server order book is empty\n", file=server_output)
                            text = "Server order book is empty"
                            server_msg = json.dumps({'Server':serverID, 'Response':text, 'Status':'Done'})
                        else:
                            server_msg = json.dumps(order_table.loc[order_table['Symbol'].isin(list(msg_data["Symbol"].split(',')))].to_json(orient='table'))
                    else:
                        print("Bad message, missing symbol\n", file=server_output)
                        text = "Bad message, missing symbol"
                        server_msg = json.dumps({'Server':serverID, 'Response':text, 'Status':'Done'})
                    server_packet.m_data = server_msg
                    client.send(server_packet.serialize())
                    data = json.loads(server_packet.m_data)
                    print(data, file=server_output)  
                    
                elif msg_type == PacketTypes.MARKET_STATUS_REQ.value:
                    server_packet.m_type = PacketTypes.MARKET_STATUS_RSP.value
                    server_msg = json.dumps({'Server':serverID, 'Status':market_status, 'Market_Period':market_period})
                    server_packet.m_data = server_msg
                    client.send(server_packet.serialize())
                    data = json.loads(server_packet.m_data)
                    print(data, file=server_output)  
                    
                elif msg_type == PacketTypes.NEW_ORDER_REQ.value:
                    server_packet.m_type = PacketTypes.NEW_ORDER_RSP.value
                    
                    if market_status == "Market Closed":
                        msg_data["Status"] = "Order Reject"
                        server_msg = json.dumps(msg_data)
                        server_packet.m_data = server_msg
                        client.send(server_packet.serialize())
                        data = json.loads(server_packet.m_data)
                        print(data, file=server_output)

                    mutex.acquire()
                    
                    #TODO Possible issue with price comparison
                    '''
                    Actually it is due to floating comparison:

                        row["Price"] <= float(msg_data["Price"]

                        On the order book:

                        'OrderIndex': 226, 'Symbol': 'XOM', 'Side': 'Sell', 'Price': 39.45, 'Qty': 2632036, 'Status': 'New'

                        When an order at 39.45 was sent,

                        {'Client': 'client1', 'OrderIndex': '1', 'Status': 'New Order', 'Symbol': 'XOM', 'Type': 'Lmt', 'Side': 'Buy', 'Price': '39.45', 'Qty': '100'}

                        {'Client': 'client1', 'OrderIndex': '1', 'Status': 'Order Reject', 'Symbol': 'XOM', 'Type': 'Lmt', 'Side': 'Buy', 'Price': 39.45, 'Qty': '100'}

                        As the price is floating number, actually it could be a little less than 39.45, so the order was rejected.

                        When an order at 39.46 was sent, we got a fill:

                        {Client': 'client1', 'OrderIndex': '2', 'Status': 'New Order', 'Symbol': 'XOM', 'Type': 'Lmt', 'Side': 'Buy', 'Price': '39.46', 'Qty': '100'}
                        {'Client': 'client1', 'OrderIndex': '2', 'Status': 'Order Fill', 'Symbol': 'XOM', 'Type': 'Lmt', 'Side': 'Buy', 'Price': 39.45, 'Qty': '100', 'ServerOrderID': 226}

                        So the best way is to convert the price to integer for comparison
                    '''
                                        
                    if (("Symbol" not in msg_data) or (msg_data["Symbol"] == "")) or \
                        (("Side" not in msg_data) or (msg_data["Side"] == "")) or \
                        (("Type" not in msg_data) or (msg_data["Type"] == "")) or \
                        (("Price" not in msg_data) or (msg_data["Type"] == "Lmt" and msg_data["Price"] == "")) or \
                        (("Qty" not in msg_data) or (msg_data["Qty"] == "") or int(msg_data["Qty"]) < 1):
                        print("Bad message, missing critical data item\n", file=server_output)
                        text = "Bad message, missing critial item"
                        server_msg = json.dumps({'Server':serverID, 'Response':text, 'Status':'Done'}) 
                        server_packet.m_type = PacketTypes.NEW_ORDER_RSP.value
                        server_packet.m_data = server_msg
                        client.send(server_packet.serialize())
                        data = json.loads(server_packet.m_data)
                        print(data, file=server_output)
                     
                    if msg_data["Type"] == "Lmt":
                        
                        msg_order_qty = int(msg_data["Qty"])
                        
                        for (index, row) in order_table.iterrows():
                            if msg_data["Status"] == "Order Fill":
                                break
                            
                            if ((row["Symbol"] == msg_data["Symbol"]) and
                                (row["Side"] != msg_data["Side"]) and
                                (row["Price"] <= float(msg_data["Price"]) if msg_data["Side"] == "Buy" else row["Price"] >= float(msg_data["Price"])) and
                               (row["Status"] != "Filled") & (row["Status"] != "Open Trade") & (int(row["Qty"]) > 0)):
                                order_qty = int(row['Qty'])
                                
                                if (order_qty == msg_order_qty):
                                    order_table.loc[index, 'Qty'] = 0
                                    order_table.loc[index, 'Status'] = 'Filled'
                                    msg_data["Price"] = round(order_table.loc[index, 'Price'], 2)
                                    msg_data['Qty'] = str(msg_order_qty)
                                    msg_data["Status"] = "Order Fill"
                                    
                                elif (order_qty< msg_order_qty):
                                    order_table.loc[index, 'Qty'] = 0
                                    order_table.loc[index, 'Status'] = 'Filled'
                                    msg_data["Price"] = round(order_table.loc[index, 'Price'], 2)
                                    msg_data['Qty'] = str(order_qty)
                                    msg_data["Status"] = "Order Partial Fill"
                                    msg_order_qty -= order_qty
                                else:
                                    order_table.loc[index, 'Qty'] -= msg_order_qty
                                    order_table.loc[index, 'Status'] = 'Partial Filled'
                                    msg_data["Price"] = round(order_table.loc[index, 'Price'], 2)
                                    msg_data['Qty'] = str(msg_order_qty)
                                    msg_data["Status"] = "Order Fill"
                                
                                msg_data["ServerOrderID"] = order_table.loc[index, 'OrderIndex']
                                server_msg = json.dumps(msg_data)
                                server_packet.m_type = PacketTypes.NEW_ORDER_RSP.value
                                server_packet.m_data = server_msg
                                client.send(server_packet.serialize())
                                data = json.loads(server_packet.m_data)
                                print(data, file=server_output)
                        
                        if msg_data["Status"] == "New Order":
                            msg_data["Status"] = "Order Reject"
                            msg_data["Price"] = round(float(msg_data["Price"]), 2)
                            server_msg = json.dumps(msg_data)
                            server_packet.m_type = PacketTypes.NEW_ORDER_RSP.value
                            server_packet.m_data = server_msg
                            client.send(server_packet.serialize())
                            data = json.loads(server_packet.m_data)
                            print(data, file=server_output)
                     
                    elif msg_data["Type"] == "Mkt":
                        
                        msg_order_qty = int(msg_data["Qty"])
                        
                        while ((order_table["Symbol"] == msg_data["Symbol"]) & 
                            (order_table["Side"] != msg_data["Side"]) & 
                            (order_table["Status"] != "Filled") & (order_table["Status"] != "Open Trade" ) & \
                            (order_table['Qty'] != 0)).any():
                            
                            if msg_data["Status"] == "Order Fill":
                                break
                            
                            mask = (order_table["Symbol"] == msg_data["Symbol"]) &  \
                                    (order_table["Side"] != msg_data["Side"]) &  \
                                    (order_table["Status"] != "Filled") & (order_table["Status"] != "Open Trade" ) & \
                                    (order_table['Qty'] != 0)
                        
                            index = -1
                            order_qty = 0
                            if msg_data["Side"] == "Sell":
                                index = order_table.loc[(mask.values), 'Price'].idxmax()
                                order_qty = int(order_table.loc[index, 'Qty'])
                            else:
                                index = order_table.loc[(mask.values), 'Price'].idxmin()
                                order_qty = int(order_table.loc[index, 'Qty'])
                            
                            if (order_qty == msg_order_qty):
                                order_table.loc[index, 'Qty'] = 0
                                order_table.loc[index, 'Status'] = 'Filled'
                                msg_data["Price"] = round(order_table.loc[index, 'Price'], 2)
                                msg_data['Qty'] = str(msg_order_qty)
                                msg_data["Status"] = "Order Fill"
                                
                            elif (order_qty < msg_order_qty):
                                order_table.loc[index, 'Qty'] = 0
                                order_table.loc[index, 'Status'] = 'Filled'
                                msg_data['Qty'] = str(order_qty)
                                msg_data["Price"] = round(order_table.loc[index, 'Price'], 2)
                                msg_data["Status"] = "Order Partial Fill"
                                msg_order_qty -= order_qty
                            else:
                                order_table.loc[index, 'Qty'] -= msg_order_qty
                                order_table.loc[index, 'Status'] = 'Partial Filled'
                                msg_data["Price"] = round(order_table.loc[index, 'Price'], 2)
                                msg_data['Qty'] = str(msg_order_qty)
                                msg_data["Status"] = "Order Fill"
                            
                            msg_data["ServerOrderID"] = order_table.loc[index, 'OrderIndex']
                            server_msg = json.dumps(msg_data)
                            server_packet.m_type = PacketTypes.NEW_ORDER_RSP.value
                            server_packet.m_data = server_msg
                            client.send(server_packet.serialize())
                            data = json.loads(server_packet.m_data)
                            print(data, file=server_output)
                            
                        if msg_data["Status"] == "New Order":
                            msg_data["Status"] = "Order Reject"
                            server_msg = json.dumps(msg_data)
                            server_packet.m_type = PacketTypes.NEW_ORDER_RSP.value
                            server_packet.m_data = server_msg
                            client.send(server_packet.serialize())
                            data = json.loads(server_packet.m_data)
                            print(data, file=server_output)
                            
                    else:
                        msg_data["Status"] = "Order Reject"
                        server_msg = json.dumps(msg_data)
                        server_packet.m_type = PacketTypes.NEW_ORDER_RSP.value
                        server_packet.m_data = server_msg
                        client.send(server_packet.serialize())
                        data = json.loads(server_packet.m_data)
                        print(data, file=server_output)
                    
                    mutex.release()
                                        
                else:
                    print("Unknown Message from Client\n", file=server_output)
                    text = "Unknown Message from Client"
                    server_msg = json.dumps({"Server":serverID, "Response":text, "Status":"Done"})
                    print(server_msg, file=server_output)
                    server_packet.m_type = PacketTypes.END_RSP.value
                    
                    server_packet.m_data = server_msg
                    client.send(server_packet.serialize())
                    data = json.loads(server_packet.m_data)
                    print(data, file=server_output)
                    
                if (server_packet.m_type == PacketTypes.END_RSP.value or (server_packet.m_type == PacketTypes.CONNECTION_RSP.value and \
                    data['Status'] == "Rejected")):
                    client.close()
                    if server_packet.m_type == PacketTypes.END_RSP.value:
                        del clients[client]
                    users = ''
                    for clientKey in clients:
                        users += clients[clientKey] + ' '
                        print(users, file=server_output)
                    return
                elif server_packet.m_type == PacketTypes.SERVER_DOWN_RSP.value:
                        Exception("Server Down")
        except (KeyboardInterrupt, KeyError, Exception):
            print("Exception in handle client", file=server_output)
            q.put(Exception("handle_client"))
            client.close()
            sys.exit(0) 
        except json.decoder.JSONDecodeError:
            q.put(Exception("handle_client"))
            client.close()
            sys.exit(0)       
         
def get_stock_list():
    enginestk = create_engine('sqlite:///TradingBBD.db',connect_args={'check_same_thread': False})
    connstk = enginestk.connect()
    stockdf = pd.read_sql_query("SELECT * from Stocks", connstk)
    stklist = list(stockdf.Ticker)
    return stklist

def generate_qty(number_of_qty):
    total_qty = 0
    list_of_qty = []
    for index in range(number_of_qty):
        qty = random.randint(1,101)
        list_of_qty.append(qty)
        total_qty += qty
    return np.array(list_of_qty)/total_qty
    
def populate_order_table(symbols, start, end):
    #price_scale = 0.05
    #price_unit = 100
    global order_index
    global order_table
    global market_status
    
    if (market_status == "Open" or market_status == "Pending Closing"):
        return
        
    symbol_list = ','.join('"' + symbol + '"' for symbol in symbols)
    select_st = "SELECT * FROM FRE_Stocks WHERE date >= " + "\"" + start + "\"" + " AND date <= " + "\"" + end + "\"" + " AND symbol in (" + symbol_list + ");"
    result_set = engine.execute(select_st)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    
    order_table.drop(order_table.index, inplace=True)
    list_of_qty_map = {}
    max_number_orders = 0
    for index, stock_data in result_df.iterrows():
        
        if stock_data['open'] > high_price_min:
            price_scale = high_price_scale
        else:
            price_scale = low_price_scale
            
        list_of_qty = generate_qty(int((float(stock_data['high'])-float(stock_data['low']))/price_scale))
        list_of_qty_map[stock_data['symbol']] = list_of_qty
        if max_number_orders < len(list_of_qty):
            max_number_orders = len(list_of_qty)
    
    if mutex.locked() is True:
        print("Is locked!")
                
    mutex.acquire()
    
    order_table.fillna(0)
    order_index = 0
    
    #print(list_of_qty_map)
    #print(max_number_orders)
    
    for index in range(0, max_number_orders-1, 2):
        for i, stock_data in result_df.iterrows(): 
           
            if index >= len(list_of_qty_map[stock_data['symbol']]):
                #print(i, index, list_of_qty_map[stock_data['symbol']])
                continue
            
            buy_price = float(stock_data['low']);
            sell_price = float(stock_data['high'])
            daily_volume = float(stock_data['volume'])
            open_price = float(stock_data['open'])
            close_price = float(stock_data['close'])
            
            if stock_data['open'] > high_price_min:
                price_scale = high_price_scale
            else:
                price_scale = low_price_scale
            
            buy_price = open_price - (index+1) * price_scale * (random.randint(1, price_unit)/price_unit)
            buy_price = float("{:.2f}".format(buy_price))
            qty = float(list_of_qty_map[stock_data['symbol']][index])
            
            order_index += 1
            order_qty = int(qty*daily_volume*(random.randint(1, price_unit)/price_unit))
            if index == 0:
                order_table.loc[order_index] = ['Srv_' + market_period + '_' + str(order_index), stock_data['symbol'], open_price, close_price,
                               'Buy' if random.randint(1,11) % 2 == 0 else 'Sell', open_price, 0, order_qty,'Open Trade']
            else:
                order_table.loc[order_index] = ['Srv_' + market_period + '_' + str(order_index), stock_data['symbol'], open_price, close_price,
                               'Buy', buy_price, order_qty, order_qty,'New']
            
            sell_price = open_price + (index+1) * price_scale * (random.randint(1, price_unit)/price_unit)
            sell_price = float("{:.2f}".format(sell_price))
            qty = float(list_of_qty_map[stock_data['symbol']][index])
            
            order_index += 1
            order_qty = int(qty*daily_volume*(random.randint(1, price_unit)/price_unit))
            order_table.loc[order_index] = ['Srv_' + market_period + '_' + str(order_index), stock_data['symbol'], open_price, close_price,
                           'Sell', sell_price, order_qty, order_qty, 'New']
         
    order_table = order_table.sort_values(['Side', 'Symbol', 'Price', 'Qty']) 
    
    mutex.release()
    
    print(order_table)
    
    
def create_market_interest(symbols):
   
    global market_period
    global order_table
    global order_index
    #print(market_status, market_period, "No new market interest")
    
    while True:
        time.sleep(order_interval_time)
        if len(order_table) != 0 and market_status == 'Open':
            
            for i in range(len(symbols)):
                
                # Some stocks may have fewer intraday data than others, 
                # it could be empty while other stocks still create intrday orders
                if intraday_order_map[market_period][i].empty == True:
                    continue
                
                symbol = symbols[i]
                
                try:
                
                    mutex.acquire()
                    
                    ### BUY logic
                    if ((order_table['Symbol'] == symbol) & (order_table['Side'] == 'Buy')).any():
                        mask = (order_table['Symbol'] == symbol) & (order_table['Side'] == 'Buy') 
                        
                        best_buy_index = order_table.loc[(mask.values), 'Price'].idxmax()
                        close_price = intraday_order_map[market_period][i].iloc[0]['close']
                        open_price = intraday_order_map[market_period][i].iloc[0]['open']
                        
                        new_buy_price = intraday_order_map[market_period][i].iloc[0]['low']
                        new_buy_price = float("{:.2f}".format(new_buy_price))
                        new_buy_qty = intraday_order_map[market_period][i].iloc[0]['volume']/2
                        new_buy_qty = int(new_buy_qty * random.uniform(0,1))
                        order_index += 1
                       
                        order_table.loc[order_index] = ['Srv_' + market_period + '_' + str(order_index), symbol, open_price, close_price,
                                               'Buy', new_buy_price, new_buy_qty, new_buy_qty, 'New']
                        
                        print(order_table.loc[order_index])
                        
                    #### Sell
                    if ((order_table['Symbol'] == symbol) & (order_table['Side'] == 'Sell')).any():
                    
                        mask = (order_table['Symbol'] == symbol) & (order_table['Side'] == 'Sell')
                        
                        best_sell_index = order_table.loc[(mask.values), 'Price'].idxmin()
                        close_price = intraday_order_map[market_period][i].iloc[0]['close']
                        open_price = intraday_order_map[market_period][i].iloc[0]['open']
                        
                        new_sell_price = intraday_order_map[market_period][i].iloc[0]['high']
                        new_sell_price = float("{:.2f}".format(new_sell_price))
                        new_sell_qty = intraday_order_map[market_period][i].iloc[0]['volume']/2
                        new_sell_qty = int(new_sell_qty * random.uniform(0,1))
                        order_index += 1
                        order_table.loc[order_index] = ['Srv_' + market_period + '_' + str(order_index), symbol, open_price, close_price,
                                               'Sell', new_sell_price, new_sell_qty, new_sell_qty, 'New']
                    
                        print(order_table.loc[order_index])
                      
                    intraday_order_map[market_period][i].drop(intraday_order_map[market_period][i].index[0] ,inplace=True)   
                        
                    while ((order_table['Symbol'] == symbol) & (order_table['Qty'] != 0)).any():
                        buy_mask = (order_table['Symbol'] == symbol) & (order_table['Qty'] != 0) & (order_table['Side'] == 'Buy')
                        sell_mask = (order_table['Symbol'] == symbol) & (order_table['Qty'] != 0) & (order_table['Side'] == 'Sell')
                        buy_prices = order_table.loc[(buy_mask.values), 'Price']
                        sell_prices = order_table.loc[(sell_mask.values), 'Price']
                        
                        if buy_prices.empty == False and sell_prices.empty == False:
                            best_buy_index = buy_prices.idxmax()
                            best_sell_index = sell_prices.idxmin()
                            best_buy_price = order_table.loc[best_buy_index, 'Price']
                            best_sell_price = order_table.loc[best_sell_index, 'Price']
                            #TODO Avoid floating point issue
                            if best_buy_price >= best_sell_price:
                                
                                if order_table.loc[best_buy_index, 'Qty'] == order_table.loc[best_sell_index, 'Qty']:
                                    order_table.loc[best_buy_index, 'Qty'] = 0
                                    order_table.loc[best_buy_index, 'Status'] = 'Filled'
                                    order_table.loc[best_sell_index, 'Qty'] = 0
                                    order_table.loc[best_sell_index, 'Status'] = 'Filled'
                                    
                                elif order_table.loc[best_buy_index, 'Qty'] > order_table.loc[best_sell_index, 'Qty']:
                                    order_table.loc[best_buy_index, 'Qty'] -= order_table.loc[best_sell_index, 'Qty']
                                    order_table.loc[best_buy_index, 'Status'] = 'Partial Filled'
                                    order_table.loc[best_sell_index, 'Qty'] = 0
                                    order_table.loc[best_sell_index, 'Status'] = 'Filled'
                                    
                                else:
                                    order_table.loc[best_sell_index, 'Qty'] -= order_table.loc[best_buy_index, 'Qty']
                                    order_table.loc[best_sell_index, 'Status'] = 'Partial Filled'
                                    order_table.loc[best_buy_index, 'Qty'] = 0
                                    order_table.loc[best_buy_index, 'Status'] = 'Filled'
                                    
                            else:
                                order_table = order_table.sort_values(['Side', 'Symbol', 'Price', 'Qty'])  
                                print(order_table)
                                break
                           
                        else:
                            order_table = order_table.sort_values(['Side', 'Symbol', 'Price', 'Qty'])  
                            print(order_table)
                            break
                        
                    mutex.release()
                
                #except (KeyboardInterrupt):
                except Exception as e:
                    print("Except in create market interest")
                    print(e)
                    if mutex.locked() == True:
                        print("Still locked")
                        mutex.release()
                    #sys.exit(0)    
                    

#TODO! The logic need to be optimized
def close_trades(symbols):
    global order_index
    global order_table
    for symbol in symbols:
        side = 'Buy' if random.randint(1,11) % 2 == 0 else 'Sell'
        if ((order_table['Symbol'] == symbol) & (order_table['Side'] == side)).any():
            mask = (order_table['Symbol'] == symbol) & (order_table['Side'] == side) & (order_table['Qty'] != 0)
            if side == 'Buy':      
                buy_prices = order_table.loc[(mask.values), 'Price']
                if buy_prices.empty == False:
                    best_buy_index = buy_prices.idxmax()
                    order_table.loc[best_buy_index, 'Qty'] = 0
                    order_table.loc[best_buy_index, 'Status'] = 'Close Trade'
                else:
                    open_price = order_table.loc[order_index-1, 'Open']
                    close_price = order_table.loc[order_index-1, 'Close']
                    qty = order_table.loc[order_index, 'OrigQty']
                    order_index += 1
                    order_table.loc[order_index] = ['Srv_' + market_period + '_' + str(order_index), symbol, open_price, close_price,
                                           'Buy', close_price, 0, qty, 'Close Trade']     
            else:      
                sell_prices = order_table.loc[(mask.values), 'Price']
                if sell_prices.empty == False:
                    best_sell_index = sell_prices.idxmin()
                    order_table.loc[best_sell_index, 'Qty'] = 0
                    order_table.loc[best_sell_index, 'Status'] = 'Close Trade'
                else:
                    open_price = order_table.loc[order_index-1, 'Open']
                    close_price = order_table.loc[order_index-1, 'Close']
                    qty = order_table.loc[order_index, 'OrigQty']
                    order_index += 1
                    order_table.loc[order_index] = ['Srv_' + market_period + '_' + str(order_index), symbol, open_price, close_price,
                                           'Sell', close_price, 0, qty, 'Close Trade']     
        
        order_table = order_table.sort_values(['Side', 'Symbol', 'Price', 'Qty'])  
        print(order_table)             
        
        #print(order_table, file = order_table_file) 
         
            
def update_market_status(status, day):
    global market_status
    global order_index
    global order_table
    market_status = status
    
    global symbols
    global market_periods
    global market_period
    
    print("day=", day)
    print(market_status, market_periods[day])
    market_period = market_periods[day]
 
    populate_order_table(symbols, market_periods[day], market_periods[day])
    market_status = 'Open'
    print(market_status)
    time.sleep(market_open_time)
    market_status = 'Pending Closing'
    print(market_status)
    time.sleep(market_pending_close_time)
    market_status = 'Market Closed'
    print(market_status)
    close_trades(symbols)
    time.sleep(market_close_time)
    
def set_market_status(scheduler, time_in_seconds):
    value = datetime.datetime.fromtimestamp(time_in_seconds)
    print(value.strftime('%Y-%m-%d %H:%M:%S'))
    for day in range(total_market_days):
        scheduler.enter((market_close_time+market_open_time+market_pending_close_time)*day+1,1, update_market_status, argument=('Pending Open',day))
    scheduler.run()

port = 6510
buf_size = 4096
fre_server = socket(AF_INET, SOCK_STREAM)
fre_server.setsockopt(IPPROTO_TCP, TCP_NODELAY, True)
print(gethostname())
fre_server.bind((gethostbyname(""), port))

location_of_pairs = 'csv/PairTrading.csv'
stock_table_name = "FRE_Stocks"
market_open_time = 75
market_pending_close_time = 3
market_close_time = 10
order_interval_time = 1
low_price_scale = 0.01
high_price_scale = 1
high_price_min = 1000
price_unit = 100
intraday_order_map = {}

clients = {}

if __name__ == "__main__":
    
    #server_output = open("server_output.txt", "w")
    server_output = sys.stderr
    
    q = queue.Queue()
    
    total_market_days = 4  # intrady data range is 30 days away from today
    order_index = 0
    
    symbols = get_stock_list()
    order_table_columns = ['OrderIndex', 'Symbol', 'Open', 'Close', 'Side', 'Price', 'Qty', 'OrigQty', 'Status']
    order_table = pd.DataFrame(columns=order_table_columns)
    order_table = order_table.fillna(0)
    order_table = pd.DataFrame(columns=order_table_columns)
    order_table = order_table.fillna(0)
    order_table['Price'] = order_table['Price'].astype(float)
    order_table['Open'] = order_table['Open'].astype(float)
    order_table['Close'] = order_table['Close'].astype(float)
    order_table['Qty'] = order_table['Qty'].astype(int)
    order_table['OrigQty'] = order_table['OrigQty'].astype(int)
    
    # USFederalHolidayCalendar has a bug, GoodFriday is not excluded
    us_bd = CustomBusinessDay(holidays=['2020-04-10'], calendar=USFederalHolidayCalendar())
  
    lastBusDay = datetime.datetime.today()
    if datetime.date.weekday(lastBusDay) == 5:      #if it's Saturday
        lastBusDay = lastBusDay - datetime.timedelta(days = 1) #then make it Friday
    elif datetime.date.weekday(lastBusDay) == 6:      #if it's Sunday
        lastBusDay = lastBusDay - datetime.timedelta(days = 2); #then make it Friday
     
    end_date = lastBusDay - datetime.timedelta(days = 1) # day before last trading day
   
    #end_date = datetime.datetime.today() - datetime.timedelta(days = 1) # yesterday
    start_date = end_date + datetime.timedelta(-total_market_days)
    
    #market_periods = pd.DatetimeIndex(pd.date_range(start=start_date.strftime("%Y-%m-%d"), end=end_date.strftime("%Y-%m-%d"), freq=us_bd)).strftime("%Y-%m-%d").tolist()
    trading_calendar = mcal.get_calendar('NYSE')
    market_periods = trading_calendar.schedule(start_date=start_date.strftime("%Y-%m-%d"), \
                                               end_date=end_date.strftime("%Y-%m-%d")).index.strftime("%Y-%m-%d").tolist()
   
    print(market_periods)
    total_market_days = len(market_periods)  # Update for remove non-trading days
    
    #market_period_objects = pd.DatetimeIndex(pd.date_range(start=start_date.strftime("%Y-%m-%d"), end=end_date.replace(hour=23, minute=30).strftime("%Y-%m-%d %H:%M:%S"), freq=us_bd)).tolist()
    market_period_objects = trading_calendar.schedule(start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d")).index.tolist()
   
    
    market_period_seconds = []
    for i in range(len(market_period_objects)):
        market_period_seconds.append(int(time.mktime(market_period_objects[i].timetuple())))   # As timestamp is 12am of each day
    market_period_seconds.append(int(time.mktime(market_period_objects[len(market_period_objects)-1].timetuple()))+24*3600)  # For last day intraday data 
    #print(market_period_objects)
    #print(market_period_seconds)
    populate_intraday_stock_data(symbols, engine, market_period_seconds)
    
    intraday_order_map, stock_market_periods = populate_intraday_order_map(symbols, engine, market_periods)
    print(intraday_order_map)
    
    print(stock_market_periods)
    for value in stock_market_periods.values():
        if total_market_days > len(value):
            total_market_days = len(value)
            market_periods = value
    
    print(market_periods)
    
    populate_stock_data(symbols, engine, stock_table_name, stock_market_periods)
   
    fre_server.listen(1)
    print("Waiting for client requests")
    try:
        scheduler = sched.scheduler(time.time, time.sleep)
        current_time_in_seconds = time.time()
        scheduler_thread = threading.Thread(target=set_market_status, args=(scheduler, current_time_in_seconds))
        #scheduler_thread.setDaemon(True)
        
        server_thread = threading.Thread(target=accept_incoming_connections, args=(q,))
        create_market_thread = threading.Thread(target=create_market_interest, args=(symbols,))
        #server_thread.setDaemon(True)
        
        scheduler_thread.start()
        server_thread.start()
        create_market_thread.start()
        
        error = q.get()
        q.task_done()
        if error is not None:
            raise error
            
        scheduler_thread.join()
        server_thread.join()
        
        fre_server.close() 
        sys.exit(0)
    
    except (KeyError, KeyboardInterrupt, SystemExit, Exception):
        print("Exception in main\n")
        fre_server.close() 
        sys.exit(0)

