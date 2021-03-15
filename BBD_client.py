# -*- coding: utf-8 -*
#!/usr/bin/env python3
#@ Copyright - 

import json
import sys

from network import PacketTypes, Packet
import queue
import threading
import pandas as pd
import numpy as np
from queue import Queue
from socket import AF_INET, socket, SOCK_STREAM, IPPROTO_TCP, TCP_NODELAY

import datetime

from sqlalchemy import create_engine
from sqlalchemy import MetaData
import time

def usd(value):
    """Format value as USD."""
    return f"${value:,.2f}"

def get_stock_selection_list():
    enginestk = create_engine('sqlite:///TradingBBD.db',connect_args={'check_same_thread': False})
    connstk = enginestk.connect()
    stockdf = pd.read_sql_query("SELECT * from Stocks", connstk)
    stklist = list(stockdf.Ticker)
    return stklist

class ClientConfig:
    def __init__(self):
        self.client_id = "client1"
        ### server side below
        self.HOST = "127.0.0.1" ### local host, local ip
        self.PORT = 6510
        self.BUF_SIZE = 4096 ### maximum message 4k
        self.ADDR = (self.HOST, self.PORT)

        self.client_socket = socket(AF_INET, SOCK_STREAM)
        self.client_socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, True)
        self.client_thread = threading.Thread()
        self.client_receiver = threading.Thread()

        self.orders = []

        self.client_thread_started = False
        self.trade_complete = False
        self.client_up = False
        self.client_symbols = ''
        self.PnL = 0
        self.Ticker_PnL = {}
        

trading_queue = queue.Queue()
trading_event = threading.Event()
client_config = ClientConfig()       
 
def receive(q=None, e=None):
    total_server_response = b'' ##in bytes
    while True:
        try:
            server_response = client_config.client_socket.recv(client_config.BUF_SIZE)
            total_server_response += server_response
            msgSize = len(total_server_response)
            while msgSize > 0: ## get sth
                if msgSize > 12: ## more than 3 int
                    server_packet = Packet()
                    server_packet.deserialize(total_server_response)
                if msgSize > 12 and server_packet.m_data_size <= msgSize: ## i have complete message
                    data = json.loads(server_packet.m_data)
                    q.put([server_packet.m_type, data]) ## pass from receiver to sender, via queue
                    total_server_response = total_server_response[server_packet.m_data_size:] ## cut the message and move on
                    msgSize = len(total_server_response)
                else: ## not enough, keep receiving more
                    server_response = client_config.client_socket.recv(client_config.BUF_SIZE)
                    total_server_response += server_response
                    msgSize = len(total_server_response)
        
            if not q.empty() and e.isSet():
                e.clear()
                
        except (OSError,Exception):  
            print("Exception in receive\n")
            sys.exit(0)

def send_msg(client_packet):
    client_config.client_socket.send(client_packet.serialize())
    data = json.loads(client_packet.m_data)
#    print(data) ## take a look
    return data

def get_response(q):
    msg_type, msg_data = q.get()
#    print(msg_data)
    if msg_data is not None:
        if msg_type == PacketTypes.END_RSP.value or msg_type == PacketTypes.SERVER_DOWN_RSP.value or \
                (msg_type == PacketTypes.CONNECTION_RSP.value and msg_data["Status"] == "Rejected"):
            client_config.client_socket.close() ## client get out
            sys.exit(0)
    return msg_data

def set_event(e):
    e.set()

def wait_for_an_event(e):
    while e.isSet():
        continue
        
def send(q=None, e=None):  
    try:
        while True:
            client_packet = Packet()
            user_input = input("Action:")
            input_list = user_input.strip().split(" ")
            if len(input_list) < 2:
                print("Incorrect Input.\n")
                continue
            ### case sensitive, won't use next week
            if "Logon" in user_input:
                client_packet.m_type = PacketTypes.CONNECTION_REQ.value ### enum here 
                client_packet.m_data = json.dumps({'Client':client_config.client_id, 'Status':input_list[0], 'Symbol':input_list[1]})
            
            elif "Client List" in user_input:
                client_packet.m_type = PacketTypes.CLIENT_LIST_REQ.value
                client_packet.m_data  = json.dumps({'Client':client_config.client_id, 'Status':input_list[0] + ' ' + input_list[1]})
                
            elif "Stock List" in user_input:
                client_packet.m_type = PacketTypes.STOCK_LIST_REQ.value
                client_packet.m_data  = json.dumps({'Client':client_config.client_id, 'Status':input_list[0] + ' ' + input_list[1]})
              
            elif "Book Inquiry" in user_input:
                if len(input_list) < 3:
                    print("Missing input item(s).\n")
                    continue
                client_packet.m_type = PacketTypes.BOOK_INQUIRY_REQ.value
                client_packet.m_data  = json.dumps({'Client':client_config.client_id, 'Status':input_list[0] + ' ' + input_list[1], 'Symbol':input_list[2]})
            
            elif "New Order" in user_input:
                if len(input_list) < 6:
                    print("Missing input item(s).\n")
                    continue
                client_packet.m_type = PacketTypes.NEW_ORDER_REQ.value
                client_packet.m_data  = json.dumps({'Client':client_config.client_id, 'Status':input_list[0] + ' ' + input_list[1], 'Symbol':input_list[2], 'Type':'Lmt', 'Side':input_list[3], 'Price':input_list[4], 'Qty':input_list[5]})
            
            elif "Client Quit" in user_input:
                client_packet.m_type = PacketTypes.END_REQ.value
                client_packet.m_data = json.dumps({'Client':client_config.client_id, 'Status':input_list[0]})
            
            else:
                print("Invalid message\n")
                continue
            
            set_event(e)
            send_msg(client_packet)
            wait_for_an_event(e)
            msg_type, msg_data = q.get()
            q.task_done()
            print(msg_data)
            if msg_data is not None:
                if msg_type == PacketTypes.END_RSP.value or msg_type == PacketTypes.SERVER_DOWN_RSP.value or \
                    (msg_type == PacketTypes.CONNECTION_RSP.value and msg_data["Status"] == "Rejected"):
                    client_config.client_socket.close()
                    sys.exit(0)
        
    except(OSError, Exception):
        q.put(PacketTypes.NONE.value, Exception('send'))
        client_socket.close()
        sys.exit(0)
       




def Client_Manual():
    try:
        
            client_config.client_socket = socket(AF_INET, SOCK_STREAM)
            client_config.client_socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, True)
            status = client_config.client_socket.connect_ex(client_config.ADDR)
            if status != 0:
                print("Fail in connecting to server")
                sys.exit(0)
                
            ## send thread and receive thread, thread is also object
            client_config.client_receiver = threading.Thread(target=receive, args=(trading_queue, trading_event)) 
            client_config.client_sender = threading.Thread(target=send, args=(trading_queue, trading_event))
            
            client_config.client_receiver.start()
            client_config.client_sender.start()
    
            if client_config.client_receiver.is_alive() is True:
                client_config.client_receiver.join()
            
            if client_config.client_sender.is_alive() is True:
                client_config.client_receiver.join()
                
    except (KeyError, KeyboardInterrupt, SystemExit, Exception):
            client_config.client_socket.close()
            sys.exit(0)
            

def logon(client_packet):
    client_config.client_symbols = ','.join([str(elem) for elem in get_stock_selection_list()]) 
    client_packet.m_type= PacketTypes.CONNECTION_REQ.value
    client_packet.m_data = json.dumps({'Client':client_config.client_id, 'Status':'Logon', 'Symbol':client_config.client_symbols})
    return client_packet

def get_client_list(client_packet):
    client_packet.m_type = PacketTypes.CLIENT_LIST_REQ.value
    client_packet.m_data  = json.dumps({'Client':client_config.client_id, 'Status':'Client List'})
    return client_packet

def get_stock_list(client_packet):
    client_packet.m_type = PacketTypes.STOCK_LIST_REQ.value
    client_packet.m_data  = json.dumps({'Client':client_config.client_id, 'Status':'Stock List'})
    return client_packet

def get_market_status(client_packet):
    client_packet.m_type = PacketTypes.MARKET_STATUS_REQ.value
    client_packet.m_data = json.dumps({'Client':client_config.client_id, 'Status':'Market Status'})
    return client_packet

def get_order_book(client_packet, symbol):
    client_packet.m_type = PacketTypes.BOOK_INQUIRY_REQ.value
    client_packet.m_data = json.dumps({'Client':client_config.client_id, 'Status':'Book Inquiry', 'Symbol':symbol})
    return client_packet

def new_order(client_packet, order_id, symbol, order_type, side, price, qty):
    if order_type == "Mkt":
        price = 0
    client_packet.m_type = PacketTypes.NEW_ORDER_REQ.value
    client_packet.m_data = json.dumps({'Client':client_config.client_id, 'OrderIndex':order_id, 'Status':'New Order', 'Symbol':symbol, 'Type':order_type, 'Side':side, 'Price':price, 'Qty':qty})
    return client_packet

def client_quit(client_packet):
    client_packet.m_type = PacketTypes.END_REQ.value
    client_packet.m_data = json.dumps({'Client':client_config.client_id, 'Status':'Client Quit'})
    return client_packet

class StocksInfo():
    def __init__(self, Ticker_, H_, K1_, Notional_, price_queue_):
        self.Ticker = Ticker_
        self.H = H_
        self.K1 = K1_
        self.Notional = Notional_
        self.price_queue = price_queue_
        self.Std = "null"
        self.MA = "null"
        self.position = 0
        self.Qty = 0
        self.current_price_buy = 0
        self.current_price_sell = 1e6
        self.Tradelist = []
        self.PnLlist = []
        self.PnL = 0
        
def StkInfo_init():
    enginestk = create_engine('sqlite:///TradingBBD.db',connect_args={'check_same_thread': False})
    connstk = enginestk.connect()
    stockdf = pd.read_sql_query("SELECT * from Stocks", connstk) ###
    stockdf.index = stockdf['Ticker']
    stock_info_dict = {stk:StocksInfo(stk,stockdf.loc[stk,'H'], stockdf.loc[stk,'K1'], stockdf.loc[stk,'Notional'], 
                                      Queue(int(stockdf.loc[stk,'H'] / 5))) for stk in stockdf['Ticker']}
    return stock_info_dict

class Trade():
    def __init__(self, Ticker_, Orders_Responses_):     ### open trade
        Orders_Responses = [response for response in Orders_Responses_ if response['Symbol'] == Ticker_]
        self.Ticker = Ticker_
        self.Postion = 1 if Orders_Responses[0]['Side'] == 'Buy' else -1
        self.ClosePrice = np.nan
        self.PnL = np.nan
        ### Open Order Filled with one order
        if len(Orders_Responses) == 1:
            Filled_order = Orders_Responses[0]
            Price = Filled_order['Price']
            Qty = int(Filled_order['Qty'])      
            self.OpenPrice = Price         
            self.Qty = Qty
        ### Open Order Filled with several orders
        else:
            PriceList = [orders['Price'] for orders in Orders_Responses]
            QtyList = [int(orders['Qty']) for orders in Orders_Responses]
            self.Qty = sum(QtyList)
            self.OpenPrice = sum([P * Q for P,Q in zip(PriceList, QtyList)]) / sum(QtyList)
        print("Open Trade in: ", self.Ticker, "With postion: ", self.Postion, "at Price: ", self.OpenPrice, "With Qty: ", self.Qty)
    def CloseTrade(self, Orders_Responses_):
        Orders_Responses = [response for response in Orders_Responses_ if response['Symbol'] == self.Ticker]
        ### Close Order Filled with one order
        if len(Orders_Responses) == 1:
            Filled_order = Orders_Responses[0]
            Price = Filled_order['Price']
            self.ClosePrice = Price
            self.PnL = (self.ClosePrice - self.OpenPrice) * self.Qty if self.Postion == 1 else (self.OpenPrice - self.ClosePrice) * self.Qty
        ### Close Order Filled with several orders
        else:
            PriceList = [orders['Price'] for orders in Orders_Responses]
            QtyList = [int(orders['Qty']) for orders in Orders_Responses]
            self.ClosePrice = sum([P * Q for P,Q in zip(PriceList, QtyList)]) / sum(QtyList)
            self.PnL = (self.ClosePrice - self.OpenPrice) * self.Qty if self.Postion == 1 else (self.OpenPrice - self.ClosePrice) * self.Qty
        print("Close Trade in: ", self.Ticker, "at Open Price: ", self.OpenPrice, "at Close Price: ", self.ClosePrice, "With Qty: ", self.Qty, "PnL: ",self.PnL)


def LogonAndTrade(q=None, e=None):
    
    client_packet = Packet()  
    set_event(e)
    send_msg(logon(client_packet))
    wait_for_an_event(e)
    get_response(q)
    
    set_event(e)
    send_msg(get_client_list(client_packet))
    wait_for_an_event(e)
    get_response(q)

    set_event(e)
    send_msg(get_stock_list(client_packet))
    wait_for_an_event(e)
    get_response(q)
    
    ### find end date
    lastBusDay = datetime.datetime.today()
    if datetime.date.weekday(lastBusDay) == 5:      #if it's Saturday
        lastBusDay = lastBusDay - datetime.timedelta(days = 1) #then make it Friday
    elif datetime.date.weekday(lastBusDay) == 6:      #if it's Sunday
        lastBusDay = lastBusDay - datetime.timedelta(days = 2)
    ##TO DO
    end_date = lastBusDay - datetime.timedelta(days = 1)  ###
    end_trading_mktperiod = end_date.strftime("%Y-%m-%d")
    
    ### initialize StkInfo Dict
    StockInfoDict = StkInfo_init()
    base_filled_orderid = []
#    sample = open('orderbook.txt', 'w') 
    OrderIndex = 0
    while True: ### outer loop
        while True: ### inner loop
            client_packet = Packet()  
            set_event(e)
            send_msg(get_market_status(client_packet))
            wait_for_an_event(e)
            ### when not empty
            while not q.empty(): 
                data = get_response(q)
                if data['Status'] not in ['Open','Pending Closing','Market Closed','Pending Open']:
                    client_config.orders.append(data)
                else:
                    mkt_status = data
#            print("mkt_status", mkt_status)
            ### wait till mkt open
            if mkt_status["Status"] == 'Open' or mkt_status["Status"] == 'Pending Closing':
                break
            if mkt_status['Market_Period'] == end_trading_mktperiod and mkt_status["Status"] == "Market Closed":
                ### calcualte PnL and stop trade Logic 1
#                TotalPnL = 0
#                for stk in StockInfoDict:
#                    TotalPnL += sum(StockInfoDict[stk].PnLlist)
#                client_config.PnL = TotalPnL 
#                ### calculate PnL for different Ticker
#                PnL_dict = {stk:usd(sum(StockInfoDict[stk].PnLlist)) for stk in StockInfoDict}
#                client_config.Ticker_PnL = PnL_dict    
                ### PnL Calculation Logic 2
                PnL_dict = {}
                for stk in StockInfoDict:
                    stkbuy_order = [order for order in client_config.orders if (order['Symbol'] == stk)&(order['Side'] == 'Buy')]
                    stkbuy_price = [order['Price'] for order in stkbuy_order]
                    stkbuy_qty = [int(order['Qty']) for order in stkbuy_order]                    
                    stksell_order = [order for order in client_config.orders if (order['Symbol'] == stk)&(order['Side'] == 'Sell')]
                    stksell_price = [order['Price'] for order in stksell_order]
                    stksell_qty = [int(order['Qty']) for order in stksell_order]
                    stkPnL = sum([P * Q for P,Q in zip(stksell_price, stksell_qty)]) - sum([P * Q for P,Q in zip(stkbuy_price, stkbuy_qty)])
                    PnL_dict.update({stk:stkPnL})
                
                client_config.PnL = sum(PnL_dict.values())
                client_config.Ticker_PnL = {stk: usd(PnL_dict[stk]) for stk in PnL_dict}
                ### complete the trade
                client_config.trade_complete = True
                break
            time.sleep(0.5)
        if client_config.trade_complete:
            break
        ### close every day in Pending Close
        if mkt_status["Status"] == "Pending Closing":
#            OrderIndex = 0
            for stk in StockInfoDict:
                stkInfo_object = StockInfoDict[stk]
                stkInfo_object.MA = 'null'
                stkInfo_object.Std = 'null'
                stkInfo_object.price_queue = Queue(int(stkInfo_object.H / 5))  # reset the members
                if stkInfo_object.position != 0:
                    client_packet = Packet()
                    OrderIndex += 1
                    client_order_id = client_config.client_id + '_' + str(OrderIndex)
                    ### if longing 
                    if stkInfo_object.position > 0:
                        new_order(client_packet, client_order_id, stk, 'Mkt', 'Sell', 100, stkInfo_object.Qty)
                        print("Close Trade in: ", stk, "With postion: Sell", "With Qty: ", stkInfo_object.Qty)
                        print("Because: Close at Pending Close.")
                        set_event(e)
                        send_msg(client_packet)
                        wait_for_an_event(e)
                        ### close trade logic
                        response_list = []
                        while not q.empty():
                            response_data = get_response(q)
                            response_list.append(response_data)
                            client_config.orders.append(response_data)
#                        Trade_object = stkInfo_object.Tradelist[-1]
#                        Trade_object.CloseTrade(response_list)
#                        stkInfo_object.PnLlist.append(Trade_object.PnL)
                        stkInfo_object.Qty = 0
                        stkInfo_object.position = 0
                        
                    ### if shorting
                    else:
                        new_order(client_packet, client_order_id, stk, 'Mkt', 'Buy', 100, stkInfo_object.Qty)
                        print("Close Trade in: ", stk, "With postion: Buy", "With Qty: ", stkInfo_object.Qty)
                        print("Because: Close at Pending Close.")
                        set_event(e)
                        send_msg(client_packet)
                        wait_for_an_event(e)
                        ### close trade logic
                        response_list = []
                        while not q.empty():
                            response_data = get_response(q)
                            response_list.append(response_data)
                            client_config.orders.append(response_data)
#                        Trade_object = stkInfo_object.Tradelist[-1]
#                        Trade_object.CloseTrade(response_list)
#                        stkInfo_object.PnLlist.append(Trade_object.PnL)
                        stkInfo_object.Qty = 0
                        stkInfo_object.position = 0
            continue ### re-enter into checking "Open" while-loop
            
        ### BBD Trading Logic
        client_packet = Packet() 
        set_event(e)
        client_msg = get_order_book(client_packet, client_config.client_symbols)
        send_msg(client_msg)
        wait_for_an_event(e)
        
        ### when not empty
        while True:
            data = get_response(q)
            if type(data) == dict:
                if data['Status']!= 'Done':
                    client_config.orders.append(data)
            else:
                break
        
        book_data = json.loads(data)
        order_book = book_data["data"]
        
#        print(order_book, file = sample)
        
        filled_order_book = [fill_orders for fill_orders in order_book if fill_orders['Status'] in ['Filled']]
        filled_orderid = [order['OrderIndex'] for order in filled_order_book]
        standing_order_book = [standing_orders for standing_orders in order_book if standing_orders['Status'] in ['New','Partial Filled']]
#        print(filled_order_book, file = sample)
#        print(standing_order_book, file = sample)
        
#        OrderIndex = 0
        
        for stk in StockInfoDict:
            standing_buy_price_list = [order['Price'] for order in standing_order_book if (order['Symbol'] == stk)&(order['Side'] == 'Buy')]
            standing_sell_price_list = [order['Price'] for order in standing_order_book if (order['Symbol'] == stk)&(order['Side'] == 'Sell')]
            StockInfoDict[stk].current_price_sell = min(standing_sell_price_list)
            StockInfoDict[stk].current_price_buy = max(standing_buy_price_list)
        
        ### store current price in price queue and use it to calculate MA and std
        for stk in StockInfoDict:
            stkInfo_object = StockInfoDict[stk]
            ### current price is based on filled_order_book
            if len(base_filled_orderid) == 0:
                current_price = (stkInfo_object.current_price_buy + stkInfo_object.current_price_sell) / 2
                base_filled_orderid = filled_orderid
            else:
                try:
                    newly_filled_orderid = [orderid for orderid in filled_orderid if orderid not in base_filled_orderid]
                    base_filled_orderid = filled_orderid
                    newly_filled_order = [order for order in filled_order_book if order['OrderIndex'] in newly_filled_orderid]
                    filled_price_list = [order['Price'] for order in newly_filled_order]
                    filled_qty_list = [int(order['OrigQty']) for order in newly_filled_order]
                    current_price = sum([P * Q for P,Q in zip(filled_price_list, filled_qty_list)]) / sum(filled_qty_list)
                except: ### when no newly filled
                    current_price = (stkInfo_object.current_price_buy + stkInfo_object.current_price_sell) / 2
#            print("current price for", stk, "P= " current_price)
            if not stkInfo_object.price_queue.full():
                stkInfo_object.price_queue.put(current_price)
                if stkInfo_object.price_queue.full():
                    stkInfo_object.MA = np.array(stkInfo_object.price_queue.queue).mean()
                    stkInfo_object.Std = np.array(stkInfo_object.price_queue.queue).std() / np.sqrt(5)
            else: # already full
                popout = stkInfo_object.price_queue.get()
                stkInfo_object.price_queue.put(current_price)
                stkInfo_object.MA = np.array(stkInfo_object.price_queue.queue).mean()
                stkInfo_object.Std = np.array(stkInfo_object.price_queue.queue).std() / np.sqrt(5)
                

        for stk in StockInfoDict:
            stkInfo_object = StockInfoDict[stk]
            K1 = stkInfo_object.K1
            MA = stkInfo_object.MA
            Std = stkInfo_object.Std
            Notional = stkInfo_object.Notional
            if MA == 'null':
                continue
            current_buy = stkInfo_object.current_price_buy
            current_sell = stkInfo_object.current_price_sell
#            current_p = (current_buy + current_sell) / 2
#            print("K1: ",K1)
#            print("MA: ",MA)
#            print("Std: ",Std)
#            print("sell p:",current_sell)
#            print("buy p:",current_buy)
            if stkInfo_object.position == 0: # not yet open position, could open
                if current_sell <= MA - K1 * Std: # below lower band, go long
                    stkInfo_object.position = 1
                    client_packet = Packet()
                    OrderIndex += 1
                    client_order_id = client_config.client_id + '_' + str(OrderIndex)
                    stkInfo_object.Qty = int(Notional / current_sell)
                    new_order(client_packet, client_order_id, stk, 'Mkt', 'Buy', 100, stkInfo_object.Qty)
                    print("Open Trade in: ", stk, "With postion: Buy", "at Price:", current_sell, "With Qty:", stkInfo_object.Qty)
                    print("Because: Price below lower band:", usd(MA - K1 * Std))
                    set_event(e)
                    send_msg(client_packet)
                    wait_for_an_event(e)
                    ### open logic
                    response_list = []
                    while not q.empty():
                        response_data = get_response(q)
                        response_list.append(response_data)
                        client_config.orders.append(response_data)
#                    Trade_object = Trade(stk, response_list)
#                    stkInfo_object.Tradelist.append(Trade_object)
            
                    
                        
                elif current_buy >= MA + K1 * Std: # above upper band, go short
                    stkInfo_object.position = -1
                    client_packet = Packet()
                    OrderIndex += 1
                    client_order_id = client_config.client_id + '_' + str(OrderIndex)
                    stkInfo_object.Qty = int(Notional / current_buy)
                    new_order(client_packet, client_order_id, stk, 'Mkt', 'Sell', 100, stkInfo_object.Qty)
                    print("Open Trade in: ", stk, "With postion: Sell", "at Price:", current_buy, "With Qty: ", stkInfo_object.Qty)
                    print("Because: Price above upper band:", usd(MA + K1 * Std))
                    set_event(e)
                    send_msg(client_packet)
                    wait_for_an_event(e)
                    ### open logic
                    response_list = []
                    while not q.empty():
                        response_data = get_response(q)
                        response_list.append(response_data)
                        client_config.orders.append(response_data)
#                    Trade_object = Trade(stk, response_list)
#                    stkInfo_object.Tradelist.append(Trade_object)
                    

            elif stkInfo_object.position == 1: # longing now
                if current_buy >= MA: # above lower bound, sell to close postion
                    client_packet = Packet()
                    OrderIndex += 1
                    client_order_id = client_config.client_id + '_' + str(OrderIndex)
                    new_order(client_packet, client_order_id, stk, 'Mkt', 'Sell', 100, stkInfo_object.Qty)
                    print("Close Trade in: ", stk, "With postion: Sell", "at Price:", current_buy, "With Qty: ", stkInfo_object.Qty)
                    print("Because: Price above lower band:", usd(MA))
                    set_event(e)
                    send_msg(client_packet)
                    wait_for_an_event(e)
                    ### close trade logic
                    response_list = []
                    while not q.empty():
                        response_data = get_response(q)
                        response_list.append(response_data)
                        client_config.orders.append(response_data)
#                    Trade_object = stkInfo_object.Tradelist[-1]
#                    Trade_object.CloseTrade(response_list)
#                    stkInfo_object.PnLlist.append(Trade_object.PnL)
                    stkInfo_object.Qty = 0
                    stkInfo_object.position = 0                  
                    
       
            else: # shorting now
                if current_sell <= MA: # below upper bound, buy to close postion
                    client_packet = Packet()
                    OrderIndex += 1
                    client_order_id = client_config.client_id + '_' + str(OrderIndex)
                    new_order(client_packet, client_order_id, stk, 'Mkt', 'Buy', 100, stkInfo_object.Qty)
                    print("Close Trade in: ", stk, "With postion: Buy", "at Price:", current_sell, "With Qty: ", stkInfo_object.Qty)
                    print("Because: Price below upper band:", usd(MA))
                    set_event(e)
                    send_msg(client_packet)
                    wait_for_an_event(e)
                    ### close trade logic
                    response_list = []
                    while not q.empty():
                        response_data = get_response(q)
                        response_list.append(response_data)
                        client_config.orders.append(response_data)
#                    Trade_object = stkInfo_object.Tradelist[-1]
#                    Trade_object.CloseTrade(response_list)
#                    stkInfo_object.PnLlist.append(Trade_object.PnL)
                    stkInfo_object.Qty = 0
                    stkInfo_object.position = 0  
                    
        time.sleep(1) ### request order book every sec
                   