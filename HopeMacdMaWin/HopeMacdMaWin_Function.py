# -*- coding: utf-8 -*-

import pandas as pd
import os
import DATA_CONSTANTS as DC
import numpy as np
import ResultStatistics as RS
import HopeMacdMaWin_Parameter as Parameter
from datetime import datetime
import time

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

def calResultByPeriod():
    '''
    按时间分段统计结果:
    1.设定开始和结束时间
    2.选择时间周期
    3.设定文件夹、买卖操作文件名、日结果文件名和要生成的新文件名
    :return:
    '''
    #设定开始和结束时间
    startdate = '2010-01-01'
    enddate = '2018-04-28'

    #2.选择时间周期
    freq='YS' #按年统计
    #freq='2QS' #按半年统计
    #freq='QS' #按季度统计
    #freq='MS' #按月统计，如需多个月，可以加上数据，比如2个月：2MS

    #3.设文件和文件夹状态
    filedir='D:\\002 MakeLive\myquant\HopeWin\Results\HopeMacdMaWin SHFE RB 3600' #文件所在文件夹
    oprfilename = 'HopeMacdMaWin SHFE.RB3600 Set26 MS4 ML21 MM5 result.csv' #买卖操作文件名
    dailyResultFileName = 'HopeMacdMaWin SHFE.RB3600 Set26 MS4 ML21 MM5 dailyresult.csv' #日结果文件名
    newFileName = 'HopeMacdMaWin SHFE.RB3600 Set26 MS4 ML21 MM5 result_by_Period_Y.csv' #要生成的新文件名
    os.chdir(filedir)
    oprdf = pd.read_csv(oprfilename)
    dailyResultdf = pd.read_csv(dailyResultFileName)

    oprdfcols = oprdf.index.tolist()
    if 'new_closeprice' in oprdfcols:
        newFlag = True
    else:
        newFlag = False

    monthlist = [datetime.strftime(x, '%Y-%m-%d %H:%M:%S') for x in list(pd.date_range(start=startdate, end=enddate, freq=freq, normalize=True, closed='right'))]

    if not startdate in monthlist[0]:
        monthlist.insert(0,startdate+" 00:00:00")
    if not enddate in monthlist[-1]:
        monthlist.append(enddate+" 23:59:59")
    else:
        monthlist[-1]=enddate+" 23:59:59"
    rlist=[]
    for i in range(1,len(monthlist)):
        starttime=monthlist[i-1]
        endtime = monthlist[i]
        startutc = float(time.mktime(time.strptime(starttime, "%Y-%m-%d %H:%M:%S")))
        endutc = float(time.mktime(time.strptime(endtime, "%Y-%m-%d %H:%M:%S")))

        resultdata = oprdf.loc[(oprdf['openutc'] >= startutc) & (oprdf['openutc'] < endutc)]
        dailydata = dailyResultdf.loc[(dailyResultdf['utc_time'] >= startutc) & (dailyResultdf['utc_time'] < endutc)]
        resultdata.reset_index(drop=True,inplace=True)
        if resultdata.shape[0]>0:
            rlist.append([starttime,endtime]+RS.getStatisticsResult(resultdata, newFlag, Parameter.ResultIndexDic, dailydata))
        else:
            rlist.append([0]*len(Parameter.ResultIndexDic))
    rdf = pd.DataFrame(rlist,columns=['StartTime','EndTime']+Parameter.ResultIndexDic)
    rdf.to_csv(newFileName)



if __name__=='__main__':
    #calDailyReturn()
    calResultByPeriod() #按时间分段统计结果