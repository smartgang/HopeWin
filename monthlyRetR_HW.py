# -*- coding: utf-8 -*-
import DATA_CONSTANTS as DC
import pandas as pd
import monthlyRetR as mrr

if __name__ == '__main__':
    #参数配置
    exchange_id = 'DCE'
    sec_id='I'
    K_MIN_SAR = 900
    K_MIN_MACD = 3600
    symbol = '.'.join([exchange_id, sec_id])
    resultfilesuffix = 'resultDSL_by_tick.csv'  # 前面不带空格,正常:result.csv,dsl:resultDSL_by_tick.csv,ownl:resultOWNL_by_tick.csv
    monthlyretrsuffix = 'monthly_retr_new.csv'  # 前面不带下划线,正常:monthly_retr.csv,双止损:monthly_retr_new.csv
    retr_col='new_ret_r' #正常:'ret_r'，双止损:'new_ret_r'
    #文件路径
    upperpath=DC.getUpperPath(uppernume=1)
    resultpath=upperpath+"\\Results\\"
    foldername = ' '.join([exchange_id, sec_id, str(K_MIN_SAR),str(K_MIN_MACD)])
    oprresultpath=resultpath+foldername
    datapath = resultpath + foldername + '\\DynamicStopLoss-22.0\\'#！！正常:'\\'，双止损：填上'\\+双止损目标文件夹\\'

    parasetlist = pd.read_csv(resultpath + 'MACDParameterSet1.csv')
    setlist = pd.Series([str(K_MIN_MACD)+' '] * parasetlist.shape[0])   #因为HP的文件名跟LW不同，多了一个MACD的周期在前面，要处理一下
    parasetlist['Setname'] = setlist.str.cat(parasetlist['Setname'])

    mrr.monthyRetR(parasetlist,datapath,symbol,K_MIN_SAR,retr_col,resultfilesuffix,monthlyretrsuffix)