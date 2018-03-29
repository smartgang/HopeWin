# -*- coding: utf-8 -*-
'''
LvyiNoKdJWin策略多进程参数优化
'''
import Hope_MACD_MA_Win
import pandas as pd
import numpy as np
import os
import DATA_CONSTANTS as DC
import multiprocessing
import HopeMacdMaWin_Parameter

def getParallelResult(symbolinfo,K_MIN,setname,rawdata,para,contractswaplist):
    result ,df ,closeopr,results = Hope_MACD_MA_Win.HopeWin_MACD_MA(symbolinfo,setname,K_MIN,rawdata,para,contractswaplist)
    result.to_csv(symbolinfo.symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    del result
    print results
    return results

if __name__=='__main__':
    #====================参数和文件夹设置======================================
    #参数设置
    strategyName=HopeMacdMaWin_Parameter.strategyName
    exchange_id = HopeMacdMaWin_Parameter.exchange_id
    sec_id = HopeMacdMaWin_Parameter.sec_id
    K_MIN = HopeMacdMaWin_Parameter.K_MIN
    startdate = HopeMacdMaWin_Parameter.startdate
    enddate = HopeMacdMaWin_Parameter.enddate
    symbol = '.'.join([exchange_id, sec_id])

    #文件路径
    upperpath=DC.getUpperPath(2)
    foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
    resultpath=upperpath+"\\Results\\"
    os.chdir(resultpath)
    try:
        os.mkdir(foldername)
    except:
        print ("%s folder already exsist!" %foldername)
    os.chdir(foldername)

    # ======================数据准备==============================================
    #取参数集
    parasetlist=pd.read_csv(resultpath+HopeMacdMaWin_Parameter.parasetname)
    paranum=parasetlist.shape[0]
    # 取合约信息
    symbolInfo = DC.SymbolInfo(symbol)

    # 取跨合约数据
    contractswaplist = DC.getContractSwaplist(symbol)
    swaplist = np.array(contractswaplist.swaputc)

    #取K线数据
    rawdata = DC.getBarData(symbol, K_MIN, startdate+' 00:00:00', enddate+' 23:59:59').reset_index(drop=True)
    # 多进程优化，启动一个对应CPU核心数量的进程池
    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    l = []
    resultlist = pd.DataFrame(columns=
                              ['Setname', 'MACD_S', 'MACD_L', 'MACD_M', 'MA_N', 'opentimes', 'end_cash', 'SR', 'Annual',
                               'Sharpe', 'DrawBack',
                               'max_single_loss_rate'])
    for i in range(0, paranum):
        setname = parasetlist.ix[i, 'Setname']
        macd_s = parasetlist.ix[i, 'MACD_Short']
        macd_l = parasetlist.ix[i, 'MACD_Long']
        macd_m = parasetlist.ix[i, 'MACD_M']
        ma_n = parasetlist.ix[i,'MA_N']
        macdParaSet = {
            'MACD_S': macd_s,
            'MACD_L': macd_l,
            'MACD_M': macd_m,
            'MA_N':ma_n
        }
        #l.append(getParallelResult(symbolInfo,K_MIN,setname,rawdata,paraSet,swaplist))
        l.append(pool.apply_async(getParallelResult,(symbolInfo,K_MIN,setname,rawdata,macdParaSet,swaplist)))
    pool.close()
    pool.join()

    # 显示结果
    i = 0
    for res in l:
        resultlist.loc[i] = res.get()
        i += 1
    print resultlist
    finalresults=("%s %d finalresults.csv"%(symbol,K_MIN))
    resultlist.to_csv(finalresults)