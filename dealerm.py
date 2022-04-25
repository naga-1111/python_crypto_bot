import ccxt
import time
import pandas as pd
import requests
from configm import Configm
from logging import getLogger,Formatter,StreamHandler,FileHandler,INFO
import datetime
import hmac
from requests import Request, Session
from ftxapim import ftxapi
import math
import pymysql.cursors
import schedule
import traceback
import gc

ftxapi = ftxapi()

class Dealerm(Configm):
    def __init__(self):
        super(Dealerm, self).__init__()
        self.logger = getLogger(__name__)
        self.logger.setLevel(INFO)
        fmt = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        h1 = StreamHandler()
        h1.setLevel(INFO)
        h1.setFormatter(fmt)
        self.logger.addHandler(h1)
        return

    #
    def ls_time(self):
        return self.ls_time
    
    #
    def time_str(self):
        time_str = f"{self.trade_time+1}" + ":00:05"
        return time_str

    #全botの共通処理部分、変数は(目標ポジションリスト、bot番号、リバランスON/OFF設定、BUSD銘柄設定)
    def dealer_main(self,mirror_sydf,bot_num,rebal_config,busd_config):
        gc.collect()
        self.retryt = 10
        while True:
            try:
                
                position_df = ftxapi.all_positions(bot_num)                            #現在のポジションdfを取得
                
                if busd_config == "ON":                                                #BUSDで取引したいbotの時だけこっちに分岐
                    position_df["symbol"] = position_df["symbol"].apply(lambda x: x.replace("BUSD","USDT"))
                else:    
                    pass
                
                try:                                                                   #現在ポジションデータと目標ポジションデータ合体
                    merged_df1 = pd.merge(mirror_sydf, position_df, on="symbol", how='outer', suffixes=['_order', '_posi'], indicator=True)
                except Exception:                                                       #現在ポジション0、目標ポジション0ならエラー、全処理終了
                    ftxapi.other_bot(bot_num)
                    print("\n********全処理完了********")
                    return
                
                common_df = ftxapi.ftx_binance_common_symbols()                 #全銘柄の最小lot情報取得
                merged_df = pd.merge(merged_df1,common_df, on="symbol")         #各銘柄の最小lot情報を結合
                
                orders_df = merged_df[merged_df["_merge"]=="left_only"].copy()          #目標ポジションのうち   自分は持ってない  銘柄リスト
                intersection_df = merged_df[merged_df["_merge"]=="both"].copy()         #目標ポジションのうち  自分も持ってる  銘柄リスト
                complement_df = merged_df[merged_df["_merge"]=="right_only"].copy()     #目標ポジションにはないけど  自分だけ持ってる  銘柄リスト

                def rebalance(x):#リバランスの量と方向を計算するための関数
                    sum_size, signal, side, size = x["sum_size"], x["signal"], x["side"], x["size"]
                    if (signal==side)&(sum_size==size):#方向も量も同じなら注文しない
                        rebal=0
                        
                    elif (signal==side)&(sum_size > size):#方向が同じで目標よりポジションが少ないなら、買い増しor売りまし でポジション増やす
                        if rebal_config=="ON":                     #リバランスモード切り替え、#バックテストではリバランスなしの方が良いbotもある
                            rebal = sum_size-size
                        else:
                            rebal = 0                         
                        
                    elif (signal==side)&(sum_size < size):#方向が同じで目標よりポジションが多いなら、一部売りor一部買い でポジション減らす
                        signal="sell" if signal=="buy" else "buy"
                        if rebal_config=="ON":
                            rebal = size-sum_size
                        else:
                            rebal = 0                         
                        
                    else:#方向が違うならドテン
                        rebal = size + sum_size

                    return rebal, signal

                try:
                    intersection_df[["sum_size", "signal"]] = intersection_df.apply(lambda x: rebalance(x), axis=1, result_type="expand")
                except Exception:   #intersection_dfが空の時はエラー、何もしない
                    pass
                
                #自分だけしかもってないポジションはシグナルを反転して閉じる
                def reverse_sig(df):
                    if df["side"]=="sell":
                        return "buy", df["size"]
                    else:
                        return "sell", df["size"]
                
                try:
                    complement_df[["signal", "sum_size"]] = complement_df.apply(reverse_sig, axis=1, result_type="expand")
                except Exception:   #complement_dfが空の時はエラー、何もしない
                    pass

                ############################################################################################################################

                df = pd.concat([complement_df, orders_df, intersection_df])     #全注文リスト作成
                df.reset_index(drop=True,inplace=True)
                if len(df)==0:
                    ftxapi.other_bot(bot_num)
                    print("\n********全処理完了********")
                    return
                df = ftxapi.min_lot_size(df)                                    #バイナンスは5USD以上でしか注文できない仕様、それを各銘柄で計算

                df["sum_size"] = df.apply(lambda x: round(x["sum_size"], x["digits"]), axis=1)              #最小注文単位で丸める
                df["TorF"] = df.apply(lambda x: True if x["sum_size"]>=x["minlot"] else False ,axis=1)      #今回の注文量と5USDどっちが大きいかを比べる
                df00 = df[(df["TorF"]==False)&(df["sum_size"]>0)]
                df = df[df["TorF"]==True]
                df.reset_index(drop=True, inplace=True)

                print("\n\n____これらの銘柄は注文量が5ドル以下のため注文出来ません____\n", df00[["symbol", "sum_size", "minlot"]]) if len(df00)!=0 else print('',end='')
                print("\n\n____全注文一覧____\n", df[["symbol","sum_size", "signal"]])
                
                if busd_config=="ON":
                    df["symbol"] = df["symbol"].apply(lambda x: x.replace("USDT","BUSD"))
                else:
                    pass

                if len(df)!=0:
                    ftxapi.gradual_order2(df)   #全銘柄について、板を見ながらスリップしすぎないように分割発注していく
                else:
                    pass

                ftxapi.other_bot(bot_num)
                print("\n********全処理完了********")
                gc.collect()

                """#一時期やってたfunding rate 回避機構
                
                df1,df2 = ftxapi.funding_rate(df)#funding rate 得側と損側で分割、損側は日付が変わってから発注
                if len(df1)!=0:
                    ftxapi.gradual_order2(df1)   #全銘柄について、板を見ながらスリップしすぎないように分割発注していく
                    ftxapi.other_bot(bot_num)
                else:
                    pass

                if len(df2)!=0:
                    print("\n=====funding rate 分割待機======\n")
                    ntime = datetime.datetime.now().minute
                    while (59>=ntime)&(ntime>=51):
                        time.sleep(1)
                        ntime = datetime.datetime.now().minute
                    print("\n=====funding rate 分割開始======\n")
                    ftxapi.chat("\n=====funding rate 分割開始======\n")
                    ftxapi.gradual_order2(df2)
                    ftxapi.other_bot(bot_num)
                else:
                    pass

                print("\n********全処理完了********")
                ftxapi.other_bot(bot_num)
                gc.collect()
                """
                return

            except Exception as e:
                traceback.print_exc()
                ftxapi.chat(f"dealer main error : {e},  retry in {self.retryt} sec")
                time.sleep(self.retryt)
                self.retryt *= 2

    #No.1
    def bot1(self):
        gc.collect()
        self.retryt = 10
        while True:
            try:
                bot_num = 1
                print("\n___bot1スタート___\n")
                mirror_sydf = ftxapi.ls_order_list()                                           #全目標ポジションリスト作成
                self.dealer_main(mirror_sydf,bot_num,self.rebal_ls_cross,"OFF")                #bot番号、目標リストに基づき全処理
                return
            except Exception as e:
                traceback.print_exc()
                ftxapi.chat(f"bybit ls error : {e},  retry in {self.retryt} sec")
                time.sleep(self.retryt)
                self.retryt *= 2

    #No.2
    def bot2(self):
        gc.collect()
        self.retryt = 10
        while True:
            try:
                bot_num = 2
                print("\n___bot2スタート___\n")
                ###################################################################################
                #データ取得
                #データベースへの接続とカーソルの生成
                self.connection = pymysql.connect(host = self.sql_host, user = self.sql_user, passwd = self.sql_pass, db = 'topps')
                self.connection.ping(reconnect=True)
                self.cursor = self.connection.cursor()
                self.cursor.execute("SELECT * FROM trader")
                rows = self.cursor.fetchall()
                
                ddf=pd.DataFrame(rows,columns=["symbol","size","net($)","Eprice","Mprice","PNL","name","time","now"])
                ddf["size"] = ddf["size"].astype(float)
                ddf["Eprice"] = ddf["Eprice"].astype(float)
                
                #ミラトレ人数増やしたい時はこのリストに増やせば良いだけ===
                names = [self.mirror_name1, self.mirror_name2, f"{self.mirror_name1}_m", f"{self.mirror_name2}_m"]
                lots = [self.mirror_lot1, self.mirror_lot2, self.mirror_lot1, self.mirror_lot2]
                #=====================================================
                mi_list = []
                for name, lot in zip(names, lots):
                    df_i = ddf[ ddf["name"]==name ]
                    df_c = df_i.copy()
                    
                    if (self.coinm==False)&("_m" in name):  #coinmモードオフなら、名前に_mがついてる人は飛ばす
                        continue
                    elif ("_m" in name):                    #coinmモードオンで、名前に_mが入ってたら、sizeを修正する
                        df_c["size"] = df_i["size"]*100/df_i["Eprice"]
                    else:
                        pass

                    if len(df_i)!=0:
                        df_c["size"] = df_c["size"].apply(lambda x: float(x)*lot)
                        mi_list.append(df_c)
                    else:
                        continue
                self.connection.close()
                #データベース通信終了####################################################################
                
                try:                #取得したデータの合体
                    ddf = pd.concat(mi_list)
                except Exception:   #mi_listの長さ0ならこっち
                    ddf = pd.DataFrame([],columns=["symbol","size","net($)","Eprice","Mprice","PNL","name","time","now"])
                
                def BUSD_USDT(x):#バイナンスドルの銘柄もテザーで取引する
                    if "USDT" in x:
                        return x
                    elif "BUSD" in x:
                        return x.replace("BUSD","USDT")
                    elif "BTCUSD" in x:#coinmの銘柄をusdmで取引する
                        return "BTCUSDT"
                    else:
                        return x
                try:
                    ddf["symbol"] = ddf["symbol"].apply(BUSD_USDT)
                except Exception:
                    pass
                
                mirror_symbols=pd.DataFrame()                                                                                   #空のdf用意
                mirror_symbols["size"] = ddf.groupby("symbol")["size"].sum()                                                    #銘柄ごとのサイズ抽出
                mirror_sydf = pd.DataFrame([mirror_symbols.index,list(mirror_symbols["size"])],index=["symbol","sum_size"]).T   #これでミラトレポジション整理
                mirror_sydf["signal"] = mirror_sydf["sum_size"].apply(lambda x: "buy" if x>0 else "sell")                       #sizeの正負をsignalという変数に記録しておく
                mirror_sydf["sum_size"] = mirror_sydf["sum_size"].apply(abs)                                                    #sizeの絶対値を取ってショートでもポジション量を正にする
                
                self.dealer_main(mirror_sydf,bot_num,"ON","OFF")                                                       #bot番号、全目標リストに基づき全処理
                return

            except Exception as e:
                traceback.print_exc()
                ftxapi.chat(f"mirror trade error : {e},  retry in {self.retryt} sec")
                time.sleep(self.retryt)
                self.retryt *= 2

    #No.3
    def cci_trade(self):
        gc.collect()
        self.retryt = 10
        while True:
            try:
                bot_num = 3
                print("\n_notebot スタート__\n")
                #cciデータとポジション合体####################################
                cci_data_df = ftxapi.cci_data()                                 #価格データ取得&cciデータ作成
                common_df = ftxapi.ftx_binance_common_symbols()                 #全銘柄の最小lot情報取得
                merged_df = pd.merge(cci_data_df, common_df, on="symbol")       #各銘柄の最小lot情報を結合
                position_df = ftxapi.all_positions(bot_num)                     #他botを排除した現在のポジション取得
                
                #cciデータとポジション合体
                df0 = pd.merge(merged_df, position_df, on="symbol", how='outer', suffixes=['_cci', '_posi'], indicator=True)
                
                #エントリーロジック#######################################################
                e_th = 0
                c_th = 0

                def flags(df):
                    if (df["cci_1"]<e_th)&(e_th<df["cci"]):#買いシグナルはe_thを上抜け、注文量をそのまま返して買い
                        return df["sum_size"], "buy"
                    
                    elif (df["cci_3"]>=c_th)&(df["_merge"]=="both"):#売りシグナルはc_thを超えてから3足時間経過、現在ポジションを全部閉じる
                        return df["size"], "sell"
                    
                    else:#シグナルが何もないなら何もしない
                        return df["sum_size"], 0
                    
                df0[["sum_size", "signal"]] = df0.apply(flags, axis=1, result_type='expand')
                order_df = df0[df0["signal"]!=0].copy()
                
                if len(order_df)==0:
                    ftxapi.other_bot(bot_num)
                    print("\n********全処理完了********")
                    return    
                
                df = ftxapi.min_lot_size(order_df)                                                          #バイナンスは5USD以上でしか注文できない仕様、それを各銘柄で計算
                df["sum_size"] = df.apply(lambda x: round(x["sum_size"], x["digits"]), axis=1)              #最小注文単位で丸める
                df["TorF"] = df.apply(lambda x: True if x["sum_size"]>=x["minlot"] else False ,axis=1)      #今回の注文量と5USDどっちが大きいかを比べる
                df0 = df[df["TorF"]==True].copy()
                df0.reset_index(drop=True, inplace=True)
                df0["symbol"] = df0["symbol"].apply(lambda x: x.replace("USDT","BUSD"))
                
                if len(df0)!=0:
                    print("\n____全注文一覧____\n", df0[["symbol", "sum_size", "signal"]])
                    ftxapi.gradual_order2(df0)   #全銘柄について、板を見ながらスリップしすぎないように分割発注していく
                else:
                    pass

                ftxapi.other_bot(bot_num)
                print("\n********全処理完了********")
                gc.collect()
                return
            except Exception as e:
                traceback.print_exc()
                ftxapi.chat(f"cci trade error : {e},  retry in {self.retryt} sec")
                time.sleep(self.retryt)
                self.retryt *= 2

    #No.4
    def bot4(self):
        gc.collect()
        self.retryt = 10
        while True:
            try:
                bot_num = 4
                print("\n___bot4スタート___\n")
                mirror_sydf = ftxapi.ls_2()                                                   #全目標ポジションリストを作成
                self.dealer_main(mirror_sydf,bot_num,self.rebal_ls_timeseri,"ON")                #bot番号、全目標リストに基づき全処理          
                #変数は目標リスト、bot番号、リバランス設定、BUSD銘柄発注設定
                return      
            except Exception as e:
                traceback.print_exc()
                ftxapi.chat(f"ls diff error : {e},  retry in {self.retryt} sec")
                time.sleep(self.retryt)
                self.retryt *= 2


if __name__ == '__main__':
    time.sleep(3)       #crontabで秒単位で指定できないけどn秒経過後にbotを動かしたかった
    dealer = Dealerm()
    #dealer.bot2()      
    dealer.bot4()      
    dealer.bot1()  
    #dealer.cci_trade()         
    

    #schedule.every(2).minutes.do(dealer.bot2)
    #schedule.every().day.at(dealer.ls_time).do(dealer.bot1)
    #schedule.every().hour.at("00:05").do(dealer.cci_trade)
    #schedule.every().day.at(dealer.time_str).do(dealer.bot4)

    #while True:
        #schedule.run_pending()
