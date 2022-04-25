import ccxt
import pandas as pd

class Configm(object):
    def __init__(self):

        self.ftx = ccxt.binance({
                "apiKey":"",
                "secret":"",
                "option":{"defaultType":"future"}})
        ##################################トレード用基本パラメータ
        self.bot_path = "/home/ec2-user/binance_3bots"  #このコードをおいてるパス、ポジションCSVとかここに保存される
        #self.bot_path = ""
        
        self.mirror_name1 = "*"             #
        self.mirror_name2 = ""              #
        self.mirror_lot1 = 0.0007           #
        self.mirror_lot2 = 0.0004           #
        self.dbg = "OFF"                    #デバッグモード実際に注文しないならON、本稼働ならOFF
        self.leverage = 20                  #レバレッジ設定
        self.coinm = False                   #coinmモード、Trueならcoinmやる、Falseならcoinmやらない        
        
        ###クロスセクションのbot
        self.bybitls_usd = 4000              #クロスセクションbybitlsロジックbotで1銘柄に何ドル持つか
        self.ls_time = "16:00:04"              #自分の環境の時刻で何時にlsのポジションチェックをするか（UTC16時が良い？）crontabで動かすなら関係ない
        self.symboln = 10                   #片側何銘柄ずつポジションを持つか
        self.rebal_ls_cross = "OFF"         #リバランスON/OFF（基本はOFFで稼働しっぱなしで、ロット上げ下げするときは、一回トレード完了するまでONにしておく）

        ###タイムシリーズのbot
        self.ls_diff_usd = 32000              #タイムシリーズls_diff、現先乖離のパラメータ、何ドルポジションを持つか
        self.long_only="OFF"                #ONにするとロングしかしないbotになる
        self.trade_time = 15                #JST1時、UTC16時にトレードするなら16-1=15を入れる（自分のサーバの時刻がJSTでも世界標準時で入れる）
        self.rebal_ls_timeseri = "OFF"      #リバランスON/OFF（基本はOFFで稼働しっぱなしで、ロット上げ下げするときは、一回トレード完了するまでONにしておく）
        
        ###有料noteのbot
        self.cci_usd = 500                   #cciロジックbotパラメータ
        
        ###価格偏差値のbot
        self.usd = 100                      #価格偏差値botパラメータ、ミラトレには関係無し     1銘柄に何ドル使うか、状況に応じて0~5銘柄注文する
        
        
        ##################################SQL通信用、Discord用パラメータ
        self.sql_host = ''
        self.sql_user = ''
        self.sql_pass = ''
        self.url = ""  #エラー通知、約定情報などのdiscordのURL
        
        ##################################あまり変えなくていいやつ
        self.cost_th = 2
        self.fraction_usd = 6  #(5ドル以上を設定しないとだめ)端数と認識するポジションの大きさ [USD]
        self.slip = 0.0005           #許容スリッページ0.05%
        self.wait = 30              #分割発注待機時間
        self.ftx.timeout = 3600*25     # 通信のタイムアウト時間の設定
