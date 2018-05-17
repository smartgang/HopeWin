# -*- coding: utf-8 -*-

import pandas as pd
import os
import DATA_CONSTANTS as DC
import numpy as np
import ResultStatistics as RS
import HopeMacdMaWin_Parameter as Parameter
def calDailyReturn():
    '''基于已有的opr结果，重新补充计算dailyReturn'''
    startdate='2016-01-01'
    enddate = '2018-04-30'
    symbol = 'SHFE.RB'
    K_MIN = 3600
    symbolinfo = DC.SymbolInfo(symbol)
    strategyName = Parameter.strategyName
    rawdata = DC.getBarData(symbol, K_MIN, startdate + ' 00:00:00', enddate + ' 23:59:59').reset_index(drop=True)
    dailyK = DC.generatDailyClose(rawdata) #生成按日的K线

    upperpath=DC.getUpperPath(Parameter.folderLevel)
    resultpath = upperpath + Parameter.resultFolderName
    os.chdir(resultpath)
    foldername = ' '.join([strategyName, Parameter.exchange_id, Parameter.sec_id, str(K_MIN)])
    #foldername = ' '.join([strategyName, Parameter.exchange_id, Parameter.sec_id, str(K_MIN)])+'\\DynamicStopLoss-18.0\\'
    #foldername = ' '.join([strategyName, Parameter.exchange_id, Parameter.sec_id, str(K_MIN)])+'\\OnceWinNoLoss8.0\\'
    #foldername = ' '.join([strategyName, Parameter.exchange_id, Parameter.sec_id, str(K_MIN)])+'\\dsl_-0.020ownl_0.008\\'
    os.chdir(foldername)
    parasetlist = pd.read_csv(resultpath + Parameter.parasetname)
    paranum = parasetlist.shape[0]

    filesuffix='result.csv'
    #filesuffix = 'resultDSL_by_tick.csv'
    #filesuffix = 'resultOWNL_by_tick.csv'
    #filesuffix = 'result_multiSLT.csv'
    indexcols=Parameter.ResultIndexDic
    resultList=[]
    for i in range(paranum):
        setname = parasetlist.ix[i, 'Setname']
        print setname
        oprdf=pd.read_csv(strategyName + ' ' + symbolinfo.symbol + str(K_MIN) + ' ' + setname + ' '+filesuffix)
        dR=RS.dailyReturn(symbolinfo,oprdf,dailyK,Parameter.initialCash)#计算生成每日结果
        dR.calDailyResult()
        dR.dailyClose.to_csv(strategyName + ' ' + symbolinfo.symbol + str(K_MIN) + ' ' + setname + ' daily'+filesuffix)

        results = RS.getStatisticsResult(oprdf, False, indexcols, dR.dailyClose)
        print results
        resultList.append([setname] + results)  # 在这里附上setname

    resultdf = pd.DataFrame(resultList,columns=['Setname'] + indexcols)
    resultdf.to_csv("%s %s %d finalresults.csv" % (strategyName,symbol, K_MIN))

if __name__=='__main__':
    calDailyReturn()