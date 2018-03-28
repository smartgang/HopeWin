# -*- coding: utf-8 -*-
'''
月度收益for MACD,MACD+MA策略
'''
import DATA_CONSTANTS as DC
import pandas as pd
import monthlyRetR as mrr

if __name__ == '__main__':
    #参数配置
    strategyName='Hope_MACD_MA'
    exchange_id = 'DCE'
    sec_id='I'
    K_MIN = 900
    symbol = '.'.join([exchange_id, sec_id])
    resultfilesuffix = 'resultDSL_by_tick.csv'  # 前面不带空格,正常:result.csv,dsl:resultDSL_by_tick.csv,ownl:resultOWNL_by_tick.csv
    monthlyretrsuffix = 'monthly_retr_new.csv'  # 前面不带下划线,正常:monthly_retr.csv,双止损:monthly_retr_new.csv
    retr_col='new_ret_r' #正常:'ret_r'，双止损:'new_ret_r'
    #文件路径
    upperpath=DC.getUpperPath(uppernume=1)
    resultpath=upperpath+"\\Results\\"
    foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
    oprresultpath=resultpath+foldername
    datapath = resultpath + foldername + '\\DynamicStopLoss-22.0\\'#！！正常:'\\'，双止损：填上'\\+双止损目标文件夹\\'

    parasetlist = pd.read_csv(resultpath + 'MACDParameterSet1.csv')

    mrr.monthyRetR(parasetlist,datapath,symbol,K_MIN,retr_col,resultfilesuffix,monthlyretrsuffix)