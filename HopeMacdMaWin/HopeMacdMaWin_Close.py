# -*- coding: utf-8 -*-

import DynamicStopLoss as dsl
import OnceWinNoLoss as ownl
import DslOwnlClose as dslownl
import DATA_CONSTANTS as DC
import pandas as pd
import os
import numpy as np
import multiprocessing
import datetime
import HopeMacdMaWin_Parameter

def getDSL(symbolInfo,K_MIN_MACD,stoplossList,parasetlist,bar1m,barxm):
    symbol=symbolInfo.symbol
    pricetick=symbolInfo.getPriceTick()
    slip=symbolInfo.getSlip()
    allresultdf = pd.DataFrame(
        columns=['setname', 'slTarget', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                 'old_SR',
                 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                 'maxSingleLoss', 'maxSingleDrawBack'])
    allnum = 0
    paranum=parasetlist.shape[0]
    for stoplossTarget in stoplossList:

        dslFolderName = "DynamicStopLoss" + str(stoplossTarget * 1000)
        try:
            os.mkdir(dslFolderName)  # 创建文件夹
        except:
            #print 'folder already exist'
            pass
        print ("stoplossTarget:%f" % stoplossTarget)

        resultdf = pd.DataFrame(
            columns=['setname', 'slTarget', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                     'old_SR',
                     'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR', 'maxSingleLoss',
                     'maxSingleDrawBack'])
        numlist = range(0, paranum, 200)
        numlist.append(paranum)
        for n in range(1,len(numlist)):#按200个分组
            pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
            l = []
            for sn in range(numlist[n-1],numlist[n]):#小组里循环
                print sn
                setname = parasetlist.ix[sn,'Setname']
                #l.append(dsl.dslCal(symbol, K_MIN_MACD,setname, bar1m, barxm,pricetick,slip, stoplossTarget, dslFolderName + '\\'))
                l.append(pool.apply_async(dsl.dslCal,(symbol,K_MIN_MACD, setname, bar1m, barxm,pricetick,slip, stoplossTarget, dslFolderName + '\\')))
            pool.close()
            pool.join()

            i=numlist[n-1]
            for res in l:
                resultdf.loc[i]=res.get()
                allresultdf.loc[allnum]=resultdf.loc[i]
                i+=1
                allnum+=1
        resultdf['cashDelta']=resultdf['new_endcash']-resultdf['old_endcash']
        resultdf.to_csv(dslFolderName+'\\'+symbol+str(K_MIN_MACD)+' finalresult_dsl'+str(stoplossTarget)+'.csv')

    allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv(symbol + str(K_MIN_MACD)+' finalresult_dsl.csv')

def getOwnl(symbolInfo,K_MIN_MACD,winSwitchList,nolossThreshhold,parasetlist,bar1m,barxm):
    symbol=symbolInfo.symbol
    slip=symbolInfo.getSlip()
    ownlallresultdf = pd.DataFrame(
        columns=['setname', 'winSwitch', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                 'old_SR',
                 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                 'maxSingleLoss', 'maxSingleDrawBack'])
    allnum=0
    for winSwitch in winSwitchList:
        ownlFolderName = "OnceWinNoLoss" + str(winSwitch * 1000)
        try:
            os.mkdir(ownlFolderName)  # 创建文件夹
        except:
            #print "dir already exist!"
            pass
        print ("OnceWinNoLoss WinSwitch:%f" % winSwitch)

        pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
        l = []
        for sn in range(0, paranum):
            setname = parasetlist.ix[sn,'Setname']
            l.append(pool.apply_async(ownl.ownlCal,(symbol,K_MIN_MACD,setname, bar1m, barxm, winSwitch, nolossThreshhold, slip,
                         ownlFolderName + '\\')))
        pool.close()
        pool.join()

        ownlresultdf = pd.DataFrame(columns=['setname', 'winSwitch', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                     'old_SR','new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR','maxSingleLoss', 'maxSingleDrawBack'])

        i = 0
        for res in l:
            ownlresultdf.loc[i] = res.get()
            ownlallresultdf.loc[allnum] = ownlresultdf.loc[i]
            i += 1
            allnum += 1

        ownlresultdf['cashDelta'] = ownlresultdf['new_endcash'] - ownlresultdf['old_endcash']
        ownlresultdf.to_csv(ownlFolderName + '\\' + symbol + str(K_MIN_MACD) + ' finalresult_ownl' + str(winSwitch) + '.csv')

    ownlallresultdf['cashDelta'] = ownlallresultdf['new_endcash'] - ownlallresultdf['old_endcash']
    ownlallresultdf.to_csv(symbol +str(K_MIN_MACD)+' finalresult_ownl.csv')

def getDslOwnl(symbolInfo,K_MIN_MACD,parasetlist,stoplossList,winSwitchList):
    symbol=symbolInfo.symbol
    slip=symbolInfo.getSlip()

    allresultdf = pd.DataFrame(
        columns=['setname', 'dslTarget', 'ownlWinSwtich', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                 'old_SR', 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                 'dslWorknum', 'ownlWorknum', 'dslRetDelta', 'ownlRetDelta'])
    allnum=0
    for stoplossTarget in stoplossList:
        for winSwitch in winSwitchList:
            dslFolderName = "DynamicStopLoss" + str(stoplossTarget * 1000) + '\\'
            ownlFolderName = "OnceWinNoLoss" + str(winSwitch * 1000) + '\\'
            newfolder = ("dsl_%.3f_ownl_%.3f" % (stoplossTarget, winSwitch))
            try:
                os.mkdir(newfolder)  # 创建文件夹
            except:
                # print newfolder, ' already exist!'
                pass
            print ("slTarget:%f ownlSwtich:%f" % (stoplossTarget, winSwitch))
            pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
            l = []
            for sn in range(0, paranum):
                setname = parasetlist.ix[sn, 'Setname']
                l.append(pool.apply_async(dslownl.dslAndownlCal,
                                                  (symbol, K_MIN_MACD,setname, stoplossTarget, winSwitch, slip, dslFolderName,
                                                   ownlFolderName, newfolder + '\\')))
            pool.close()
            pool.join()

            resultdf = pd.DataFrame(
                columns=['setname', 'dslTarget', 'ownlWinSwtich', 'old_endcash', 'old_Annual', 'old_Sharpe',
                         'old_Drawback',
                         'old_SR', 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                         'dslWorknum', 'ownlWorknum', 'dslRetDelta', 'ownlRetDelta'])
            i = 0
            for res in l:
                resultdf.loc[i] = res.get()
                allresultdf.loc[allnum] = resultdf.loc[i]
                i += 1
                allnum+=1
            resultfilename = ("%s%d finalresult_dsl%.3f_ownl%.3f.csv" % (symbol, K_MIN_MACD, stoplossTarget, winSwitch))
            resultdf.to_csv(newfolder + '\\' + resultfilename)

    allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv(symbol + str(K_MIN_MACD)+ ' finalresult_dsl_ownl.csv')


if __name__=='__main__':
    #参数配置
    strategyName=HopeMacdMaWin_Parameter.strategyName
    exchange_id = HopeMacdMaWin_Parameter.exchange_id
    sec_id = HopeMacdMaWin_Parameter.sec_id
    K_MIN = HopeMacdMaWin_Parameter.K_MIN
    startdate = HopeMacdMaWin_Parameter.startdate
    enddate = HopeMacdMaWin_Parameter.enddate
    symbol = '.'.join([exchange_id, sec_id])

    symbolinfo=DC.SymbolInfo(symbol)
    slip=DC.getSlip(symbol)
    pricetick=DC.getPriceTick(symbol)

    #计算控制开关
    calcDsl = HopeMacdMaWin_Parameter.calcDsl_close
    calcOwnl = HopeMacdMaWin_Parameter.calcOwnl_close
    calcDslOwnl = HopeMacdMaWin_Parameter.calcDslOwnl_close

    #优化参数
    dslStep = HopeMacdMaWin_Parameter.dslStep_close
    stoplossList = np.arange(HopeMacdMaWin_Parameter.dslTargetStart_close, HopeMacdMaWin_Parameter.dslTargetEnd_close, dslStep)
    ownlStep = HopeMacdMaWin_Parameter.ownlStep_close
    winSwitchList = np.arange(HopeMacdMaWin_Parameter.ownlTargetStart_close,HopeMacdMaWin_Parameter.ownltargetEnd_close, ownlStep)
    nolossThreshhold = HopeMacdMaWin_Parameter.nolossThreshhold_close * pricetick

    #文件路径
    upperpath=DC.getUpperPath(2)
    foldername = ' '.join([strategyName,exchange_id, sec_id,str(K_MIN)])
    resultpath = upperpath + "\\Results\\"
    oprresultpath=resultpath+foldername

    #原始数据处理
    bar1m=DC.getBarData(symbol=symbol,K_MIN=60,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
    barxm=DC.getBarData(symbol=symbol,K_MIN=K_MIN,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
    #bar1m计算longHigh,longLow,shortHigh,shortLow
    bar1m['longHigh']=bar1m['high']
    bar1m['shortHigh']=bar1m['high']
    bar1m['longLow']=bar1m['low']
    bar1m['shortLow']=bar1m['low']
    bar1m['highshift1']=bar1m['high'].shift(1).fillna(0)
    bar1m['lowshift1']=bar1m['low'].shift(1).fillna(0)
    bar1m.loc[bar1m['open']<bar1m['close'],'longHigh']=bar1m['highshift1']
    bar1m.loc[bar1m['open']>bar1m['close'],'shortLow']=bar1m['lowshift1']

    os.chdir(oprresultpath)
    parasetlist=pd.read_csv(resultpath+HopeMacdMaWin_Parameter.parasetname)
    paranum=parasetlist.shape[0]

    if calcDsl:
        getDSL(symbolinfo, K_MIN, stoplossList, parasetlist, bar1m,barxm)

    if calcOwnl:
        getOwnl(symbolinfo,K_MIN,winSwitchList,nolossThreshhold,parasetlist,bar1m,barxm)

    if calcDslOwnl:
        getDslOwnl(symbolinfo,K_MIN,parasetlist,stoplossList,winSwitchList)