# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 22:39:06 2020

@author: 63438
"""
import pandas as pd
from flask import Flask, render_template, request

from BBDModel import stk_select, build_trading_model, populate_backtest_data, backTest, probation_test, execute_sql_statement, populate_strategy_bt
from BBD_client import *

from network import PacketTypes, Packet
import queue
import threading
from socket import AF_INET, socket, SOCK_STREAM, IPPROTO_TCP, TCP_NODELAY


def usd(value):
    """Format value as USD."""
    return f"${value:,.2f}"

def FrontEnd(engine): 
    app = Flask(__name__)
    selected_list = []
    @app.route('/')
    def index():
        return render_template("index.html")
    
    
    @app.route('/Stock_selection')
    def Stock_selection():
        slist, sdf = stk_select()
        for i in slist:
            selected_list.append(i)
        stk_df = pd.DataFrame(sdf)
        stk_df['symbol'] = stk_df.index
        stk_df['std'] = stk_df['std'].map('{:.4f}'.format)
        stk_df = stk_df.transpose()
        list_of_stk = [stk_df[i] for i in stk_df]
        return render_template("stock_selection.html", stock_list=list_of_stk)
    
    
    @app.route('/Train_model')
    def train_model():
        stocksdf = build_trading_model(selected_list)
        populate_backtest_data(stocksdf)
        stocksdf['Profit_Loss_in_Training'] = stocksdf['Profit_Loss_in_Training'].map('${:,.2f}'.format)
        stocksdf = stocksdf.transpose()
        train_results = [stocksdf[i] for i in stocksdf]
        return render_template("train_model.html", train_list=train_results)
    
    
    @app.route('/Back_test')
    def model_back_testing():
        conn = engine.connect()
        backTest()
        populate_strategy_bt()
        result_df = execute_sql_statement("SELECT * from Stocks", conn)
        total = result_df['Profit_Loss'].sum()
        result_df['Profit_Loss'] = result_df['Profit_Loss'].map('${:,.2f}'.format)
        result_df = result_df.transpose()
        trade_results = [result_df[i] for i in result_df]
        return render_template("back_test_result.html", bt_list=trade_results, total=usd(total))
    
    
    @app.route('/Probation_test', methods = ['POST', 'GET'])
    def model_probation_testing():
        if request.method == 'POST':
            
            form_input = request.form
            probation_testing_start_date = form_input['Start Date']
            probation_testing_end_date = form_input['End Date']
    
            result_df = probation_test(probation_testing_start_date, probation_testing_end_date)
            total = result_df['Profit_Loss'].sum()
            result_df['Profit_Loss'] = result_df['Profit_Loss'].map('${:,.2f}'.format)
            result_df = result_df.transpose()
            trade_results = [result_df[i] for i in result_df]
            return render_template("probation_test_result.html", trade_list=trade_results, total=usd(total))
        else:
           
            return render_template("probation_test.html")
    
    @app.route('/start_trading',methods = ['POST', 'GET'])
    def start_trading():
        if request.method == 'POST':
            if request.form.get("Auto Trading"):
                client_config.client_socket = socket(AF_INET, SOCK_STREAM)
                client_config.client_socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, True)
                status = client_config.client_socket.connect_ex(client_config.ADDR)
                if status != 0:
                    print("Fail in connecting to server")
                    sys.exit(0)
                    
                ## send thread and receive thread, thread is also object
                client_config.client_receiver = threading.Thread(target=receive, args=(trading_queue, trading_event)) 
                client_config.client_sender = threading.Thread(target=LogonAndTrade, args=(trading_queue, trading_event))
                
                client_config.client_receiver.start()
                client_config.client_sender.start()
        
                while not client_config.trade_complete:
                    continue
    #            client_packet = Packet()
    #            send_msg(get_stock_list(client_packet))
    #            data1 = get_response(trading_queue)
                return render_template("auto_trading.html",trading_results=client_config.orders, total=usd(client_config.PnL)) ##client_config.orders
            elif request.form.get("Analysis"):
                return render_template("auto_trading_analysis.html",trading_results=client_config.Ticker_PnL)
        else:
            return render_template("start_trading.html")
    
    
    @app.route('/manual_trading',methods = ['POST', 'GET'])
    def manual_trading():
        if request.method == 'POST':           
            form_input = request.form
            print(form_input)
            trading_id = form_input['OrderId']
            trading_Ticker = form_input['Symbol']
            trading_Side = form_input['Side']
            trading_Price = form_input['Price']
            trading_Quantity = form_input['Quantity']
            ### edit the packet
            client_packet = Packet()
            client_msg = new_order(client_packet, trading_id, trading_Ticker, 'Lmt', trading_Side, trading_Price, trading_Quantity)
            send_msg(client_msg)
            data = get_response(trading_queue)
            while not trading_queue.empty():
                client_config.orders.append(get_response(trading_queue))
            return render_template("trading_results.html", trading_results=data)
        else:
           
            return render_template("manual_trading.html")
    
    @app.route('/client_down')
    def client_down():
        client_packet = Packet()
        msg_data = {}
        try:
            send_msg(client_quit(client_packet))
            msg_type, msg_data = trading_queue.get()
            trading_queue.task_done()
            print(msg_data)
            return render_template("client_down.html", server_response=msg_data)
        except(OSError, Exception):
            print(msg_data)
            return render_template("client_down.html", server_response=msg_data)
        
    return app
    