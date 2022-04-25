import ccxt
import time
import pandas as pd
import requests
from configm import Configm
from logging import getLogger,Formatter,StreamHandler,FileHandler,INFO
import datetime
import hmac
from requests import Request, Session
import json
import math
from tqdm import tqdm
import traceback


class ftxapi(Configm):
    def __init__(self):
        super(ftxapi, self).__init__()
        self.logger = getLogger(__name__)
        self.logger.setLevel(INFO)
        fmt = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        h1 = StreamHandler()
        h1.setLevel(INFO)
        h1.setFormatter(fmt)
        self.logger.addHandler(h1)
        return

    #discord通知関数
    def chat(self, text):
        webhook_url  = self.url
        main_content = {'content': f'{text}'}
        headers      = {'Content-Type': 'application/json'}
        while True:
            try:
                response = requests.post(webhook_url, json.dumps(main_content), headers=headers)
                return
            except Exception as e:
                traceback.print_exc()
                time.sleep(5)
                return

    #新規注文発注関数(銘柄も変数で入力)
    def make_new_order2(self,symbol,order_type,bs,qua):
        self.retryt = 10
        while True:
            try:
                if self.dbg == "OFF":
                    response = self.ftx.fapiPrivate_post_order({
                        "symbol": symbol,
                        "type": order_type.upper(),
                        "side": bs.upper(),
                        "quantity": qua,
                        #"price": pri,  #指値限定
                        #"timeInForce":"GTC",   #指値限定
                        #"reduceOnly": False,
                        })
                    self.chat(f"{symbol} {bs} {qua} {order_type}")
                else:
                    pass
                print(f"{symbol} {bs} {qua} {order_type}")
                time.sleep(0.01)
                return
            except Exception as e:
                print("make order error :\n")
                traceback.print_exc()
                self.chat(f"make order error : {e},  retry in {self.retryt} sec")
                self.chat(f"you tried to order {symbol} {bs} {qua} {order_type}")
                time.sleep(self.retryt)
                self.retryt *= 2

    #板を見る
    def min_max_order2(self,symbol):
        self.retryt = 10
        while True:
            try:
                response = requests.get(f"https://www.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000")
                sell_df = pd.DataFrame(response.json()["bids"], columns=["sell price", "sell amounts"])
                buy_df = pd.DataFrame(response.json()["asks"], columns=["buy price", "buy amounts"])
                book_df = pd.concat([buy_df, sell_df],axis=1)
                book_df = book_df.astype(float)
                #print(book_df)
                return book_df
            except Exception as e:
                print(f"order book depth error :\n")
                traceback.print_exc()
                self.chat(f"order book depth error : {e},  retry in {self.retryt} sec")
                time.sleep(self.retryt)
                self.retryt *= 2

    #各銘柄の最小注文lotを計算する（バイナンスは全銘柄5USD以上の注文しかできない仕様）
    def min_lot_size(self, df):
        def pri(symbol):
            while True:
                try:                            
                    api = requests.get(f"https://www.binance.com/fapi/v1/klines?symbol={symbol}&interval=1m&limit=1").json()
                    time.sleep(0.1)
                    price = float(api[0][4])
                    minlot = self.fraction_usd//price
                    return minlot
                except Exception as e:
                    traceback.print_exc()
                    time.sleep(10)
        df["5USD lot"] = df["symbol"].apply(lambda x: pri(x))
        df["minlot"] = df.apply(lambda x: max(x["minsize"],x["5USD lot"]) ,axis=1)
        df.drop("5USD lot",axis=1,inplace=True)
        return df

    #現在のポジションを取る関数
    def all_positions(self,num):
        self.retryt = 10
        while True:
            try:
                df = self.adj_position(num)
                df["side"] = df["size"].apply(lambda x: "buy" if x>=0 else "sell")
                df["size"] = df["size"].apply(abs)
                df.reset_index(drop=True,inplace=True)
                #print(df)
                return df
            except Exception as e:
                traceback.print_exc()
                self.chat(f"get position error : {e},  retry in {self.retryt} sec")
                time.sleep(self.retryt)
                self.retryt *= 2

    #jsonから他のbotのポジションを除外する
    def adj_position(self,num):
        self.retryt = 10
        while True:
            try:
                #apiから全体ポジション取得#########################################################
                api = self.ftx.fapiPrivateGetPositionRisk()
                df_row = pd.DataFrame(api)
                df_row["positionAmt"] = df_row["positionAmt"].astype(float)
                df_row = df_row[["symbol", "positionAmt"]][df_row["positionAmt"]!=0].copy()
                df_row.rename({"positionAmt":"size"},axis=1,inplace=True)
                #print("\n___api__\n", df_row)

                #ほかのBOTのポジションをcsvから取得#########################################################
                df_csv_list = []
                for i in range(1,10):        
                    try:
                        df = pd.read_csv(f"{self.bot_path}/position_{i}.csv")
                    except Exception as e:
                        df = pd.DataFrame([],columns=["symbol", "size"])
                    df_csv_list.append(df.copy())
                
                del df_csv_list[num-1]#自分のcsvは削除
                df = pd.concat(df_csv_list)
                
                dff=pd.DataFrame()                                                                                  #空のdf用意
                dff["size"] = df.groupby("symbol")["size"].sum()                                                    #銘柄ごとのサイズ抽出
                other_df = pd.DataFrame([dff.index,list(dff["size"])],index=["symbol", "size"]).T                   #その他botの全ポジション
                #print(f"\n___{num}___\n", other_df)
                
                #apiから他のbotのポジションを削除#########################################################
                try:
                    merged_df1 = pd.merge(df_row, other_df, on="symbol", how='outer', suffixes=['_api', '_ot'])
                except Exception as e:  #どっちも空の時merge出来ない、空のdf返す
                    return pd.DataFrame([],columns=["symbol", "size"])
                merged_df1.fillna(0,inplace=True)
                merged_df1["size"] = merged_df1["size_api"] - merged_df1["size_ot"]
                merged_df = merged_df1[["symbol", "size"]][merged_df1["size"]!=0].copy()
                #print("\n___merged___\n", merged_df)
                return merged_df

            except Exception as e:
                traceback.print_exc()
                self.chat(f"get position error : {e},  retry in {self.retryt} sec")
                time.sleep(self.retryt)
                self.retryt *= 2

    #ほかのボットと連携用のポジションを出力する
    def other_bot(self,num):
        df = self.adj_position(num)
        df.to_csv(f"{self.bot_path}/position_{num}.csv",index=False)
        return

    #Binanceの全銘柄の各情報取得
    def ftx_binance_common_symbols(self):
        self.retryt = 10
        while True:
            try:
                binance_api = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()
                binance_df = pd.DataFrame(binance_api["symbols"])
                #print(binance_df)
                binance_perpdf = binance_df[(binance_df["contractType"]=="PERPETUAL")&(binance_df["quoteAsset"]=="USDT")]
                binance_sydf = pd.DataFrame()
                binance_sydf["symbol"] = binance_perpdf["symbol"]
                #print(binance_perpdf["filters"])
                binance_sydf["minsize"] = binance_perpdf["filters"].apply(lambda x: x[2]["minQty"])

                #print(binance_sydf)
                binance_sydf["minsize"] = binance_sydf["minsize"].astype(float)
                binance_sydf["floatn"] = binance_sydf["minsize"].apply(lambda x: math.modf(x)[0])
                binance_sydf["intn"] = binance_sydf["minsize"].apply(lambda x: math.modf(x)[1])
                binance_sydf["minsize"] = binance_sydf.apply(lambda x: int(x["intn"]) if x["floatn"]==0 else x["floatn"], axis=1)
                binance_sydf["digits"] = binance_sydf.apply(lambda x: 0 if x["minsize"]*2>=1 else len(str(x["minsize"]*2))-2 ,axis=1)
                #print(binance_sydf)

                binance_sydf.drop(["floatn","intn"],inplace=True,axis=1)

                return binance_sydf
            except Exception as e:
                self.chat(f"common symbol error : {e},  retry in {self.retryt} sec")
                print("common symbol error :\n")
                traceback.print_exc()
                time.sleep(self.retryt)
                self.retryt *= 2

    #レバレッジ設定
    def set_leverage(self, x):
        while True:
            try:
                res = self.ftx.fapiPrivatePostLeverage({"symbol": x, "leverage": self.leverage})
                time.sleep(0.01)
                return
            except Exception as e:
                print(x)
                traceback.print_exc()
                time.sleep(self.retryt)

    #各銘柄で板の厚さに応じて一括発注できる量を決める
    def ava_qua(self, x): 
        if x["signal"] == "buy":
            book_df = self.min_max_order2(x["symbol"]) #板の厚さ取得
            current_price = book_df.loc[0,"buy price"]
            tolerance_price = current_price*(1+self.slip)
            available_quantity = book_df["buy amounts"][book_df["buy price"]<tolerance_price].sum()
            available_quantity = round(available_quantity, x["digits"])
            time.sleep(0.01)
            return available_quantity
        else:
            book_df = self.min_max_order2(x["symbol"]) #板の厚さ取得
            current_price = book_df.loc[0,"sell price"]
            tolerance_price = current_price*(1-self.slip)
            available_quantity = book_df["sell amounts"][book_df["sell price"]>tolerance_price].sum()
            available_quantity = round(available_quantity, x["digits"])
            time.sleep(0.01)
            return available_quantity

    #データフレームに対して発注する
    def df_order(self, df):
        df = df.copy()
        df["max_order"] = df.apply(self.ava_qua, axis=1)
        df["remain"] = df.apply(lambda x: x["sum_size"]-x["max_order"], axis=1)
        df.apply(lambda x: self.make_new_order2(x["symbol"],"market",x["signal"],str(round(x["sum_size"], x["digits"]))) if x["remain"]<=0 else print('',end=''), axis=1)
        df.apply(lambda x: self.make_new_order2(x["symbol"],"market",x["signal"],str(round(x["max_order"], x["digits"]))) if x["remain"]>0 else print('',end=''), axis=1)
        
        if len(df[df["remain"]>0])==0:
            #print("これ以上分割なし")
            df = df[df["remain"]>0].copy()
            return df
        
        print(f"\n分割発注チェック {self.wait} 秒待機\n")
        
        for i in range(1,self.wait):
            if self.wait-i <= 3:
                print(f"残り{self.wait-i} 秒")
            elif (self.wait-i)%10==0:
                print(f"残り{self.wait-i} 秒")
            else:
                pass
            time.sleep(1)
        df["sum_size"] = df["remain"]
        df = df[df["remain"]>0].copy()
        return df

    #分割発注関数
    def gradual_order2(self,df):
        self.retryt = 10
        while True:
            try:
                df["symbol"].apply(self.set_leverage)#銘柄ごとにレバレッジ設定
                df = self.df_order(df)#データフレームに対して板を見ながら発注
                n = 0
                while True:
                    if len(df)==0:
                        del df
                        return
                    else:
                        n += 1
                        print(f"\n==分割発注{n}回目===\n")
                        df = self.df_order(df)
                        #print(df)
            except Exception as e:
                print("gradual order :\n")
                traceback.print_exc()
                self.chat(f"gradual order error : {e},  retry in {self.retryt} sec")
                time.sleep(self.retryt)
                self.retryt *= 2

    #全銘柄一覧
    def all_symbols(self):
        while True:
            try:
                binance_api = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo").json()
                binance_df = pd.DataFrame(binance_api["symbols"])
                binance_perpdf = binance_df[(binance_df["contractType"]=="PERPETUAL")&(binance_df["quoteAsset"]=="USDT")]
                #print(binance_perpdf["symbol"])
                return list(binance_perpdf["symbol"])
            except Exception as e:
                traceback.print_exc()
                self.chat(e)
                time.sleep(10)

    #全銘柄のapiデータ取得
    def all_ohlcv(self):
        df_list00 = []
        self.symbol_list2 = list(self.all_symbols())
        df_list = []
        for sy in self.symbol_list2:
            while True:
                try:
                    time.sleep(0.05)#2400 reqests per minute, 0.025
                    future_api = requests.get(f"https://fapi.binance.com/fapi/v1/klines?symbol={sy}&interval=1d&limit=70").json()
                    future_df = pd.DataFrame()
                    future_df["date"] = list(map(lambda x: pd.Timestamp(int(x[0]), unit="ms"), future_api))
                    future_df["symbol"] = sy
                    future_df["open"] = list(map(lambda x: float(x[1]), future_api))
                    future_df["high"] = list(map(lambda x: float(x[2]), future_api))
                    future_df["low"] = list(map(lambda x: float(x[3]), future_api))
                    future_df["close"] = list(map(lambda x: float(x[4]), future_api))
                    future_df["volume"] = list(map(lambda x: float(x[5]), future_api))
                    future_df["buy volume"] = list(map(lambda x: float(x[9]), future_api))
                    df_list.append(future_df)
                    break
                except Exception as e:
                    traceback.print_exc()
                    time.sleep(15)

            df = pd.concat(df_list)
            df.reset_index(drop=True, inplace=True)
            #print("\n_________api data_________\n",df)
            #df.to_pickle("1.pkl")
            df_list00.append(df)
            time.sleep(0.01)

        df = pd.concat(df_list00)
        df = df.sort_values(["date", "symbol"])
        df = df.drop_duplicates(subset=["date", "symbol"], keep="last")
        df = df.reset_index(drop=True)
        #df.to_pickle("all_data1d.pkl")
        #print(df)
        return df

    def cci_data(self):
        """
        this function is secret.
        """
        return
    
    def best_std(self):
        """
        this function is secret.
        """
        return

    #fundingrate取得
    def get_fr(self,sy):
        while True:
            try:
                api = requests.get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sy}").json()
                time.sleep(0.1)
                return api["lastFundingRate"]
            except Exception as e:
                print(e)
                time.sleep(10)

    #dfにfundingrate適応
    def funding_rate(self,df):
        df["fr"] = df["symbol"].apply(lambda x: float(self.get_fr(x)))#fundingrate取得
        earn = ((df["signal"]=="sell")&(df["fr"]>0))|((df["signal"]=="buy")&(df["fr"]<=0))
        pay = ((df["signal"]=="sell")&(df["fr"]<0))|((df["signal"]=="buy")&(df["fr"]>=0))

        df1 = df[earn].copy()
        df2 = df[pay].copy()
        df1.reset_index(drop=True,inplace=True)
        df2.reset_index(drop=True,inplace=True)
        return df1, df2

    #bybitの全銘柄取得
    def bybit_symbols(self):
        while True:
            try:
                api = requests.get(f"https://api.bybit.com/v2/public/tickers").json()
                df = pd.DataFrame(api["result"])
                df["symbol"] = df["symbol"].apply(lambda x: "0" if "22" in x else x)#22期限付き先物の銘柄名を文字列"0"に変える
                df["symbol"] = df["symbol"].apply(lambda x: "0" if "23" in x else x)#23期限付き先物の銘柄名を文字列"0"に変える
                scli = df["symbol"][df["symbol"] != "0"]    #期限付きじゃないやつだけを抽出
                scli_usdt = scli.apply(lambda x: x if "USDT" in x else "0") #文字列USDTが入ってないやつは削除
                list_usdt = list(scli_usdt[scli_usdt != "0"] )
                return list_usdt
            except Exception as e:
                traceback.print_exc()
                self.chat(e)
                time.sleep(10)

    
    def lsratio(self):
        """
        this function is secret.
        """
        return
    
    def ls_order_list(self):
        """
        this function is secret.
        """
        return
    
    def ls_diff(self):
        """
        this function is secret.
        """
        return
    
if __name__ == "__main__":
    ftxo = ftxapi()

    #ftxo.set_leverage()
    #ftxo.make_new_order2("BTCUSDT", "MARKET", "buy", "0.001", "30000")
    #ftxo.cancel()
    #ftxo.open_orders()
    #ftxo.min_max_order()
    #ftxo.min_max_order2("ANCUSDT")
    #ftxo.kline()
    #ftxo.long_short()
    #ftxo.all_positions(1)
    #ftxo.balance_nortify()
    #ftxo.BB_2sig()
    #ftxo.ftx_binance_common_symbols()
    #ftxo.check_position()
    #ftxo.get_flag()
    #ftxo.adj_position(1)
    #ftxo.all_ohlcv()
    #ftxo.cci_data()
    #ftxo.best_std()
    #ftxo.lsratio()
    ftxo.ls_order_list()
    #ftxo.ls_diff()