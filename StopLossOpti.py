# -*- coding: utf-8 -*-

import DynamicStopLoss as dsl
import OnceWinNoLoss as ownl
import DslOwnlClose as dslownl
import DATA_CONSTANTS as DC
import pandas as pd
import os
import numpy as np
import multiprocessing

if __name__=='__main__':
    #参数配置
    exchange_id = 'DCE'
    sec_id='I'
    symbol = '.'.join([exchange_id, sec_id])
    K_MIN_SAR = 900
    K_MIN_MACD = 3600
    slip=DC.getSlip(symbol)
    pricetick=DC.getPriceTick(symbol)
    starttime='2016-01-01'
    endtime='2017-12-31'
    #tickstarttime='2016-01-01'
    #tickendtime='2017-12-31'
    #计算控制开关
    calcDsl=True
    calcOwnl=True
    calcDslOwnl=True

    #优化参数
    dslStep=-0.002
    stoplossList = np.arange(-0.010, -0.042, dslStep)
    #stoplossList=[-0.022]
    ownlStep=0.001
    winSwitchList = np.arange(0.003, 0.011, ownlStep)
    #winSwitchList=[0.009]
    nolossThreshhold = 3 * pricetick

    #文件路径
    upperpath=DC.getCurrentPath()
    resultpath=upperpath+"\\Results\\"
    foldername = ' '.join([exchange_id, sec_id, str(K_MIN_SAR)])
    oprresultpath=resultpath+foldername

    #原始数据处理

    bar1m=DC.getBarData(symbol=symbol,K_MIN=60,starttime=starttime+' 00:00:00',endtime=endtime+' 00:00:00')
    barxm=DC.getBarData(symbol=symbol,K_MIN=K_MIN_SAR,starttime=starttime+' 00:00:00',endtime=endtime+' 00:00:00')
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

    if calcDsl:
        dslresultdf = pd.DataFrame(columns=['setname', 'slTarget','worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                                         'old_SR',
                                         'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                                         'maxSingleLoss', 'maxSingleDrawBack'])
        allnum=0
        for stoplossTarget in stoplossList:

            dslFolderName="DynamicStopLoss" + str(stoplossTarget*1000)
            try:
                os.mkdir(dslFolderName)#创建文件夹
            except:
                print 'folder already exist'
            print ("stoplossTarget:%f"%stoplossTarget)
            setname=str(K_MIN_MACD)+' HopeWin'

            l=dsl.dslCal(symbol=symbol,K_MIN=K_MIN_SAR,setname=setname,bar1m=bar1m,barxm=barxm,pricetick=pricetick,slip=slip,slTarget=stoplossTarget,tofolder=dslFolderName+'\\')
            dslresultdf.loc[allnum] = l
            allnum+=1

        dslresultdf['cashDelta'] = dslresultdf['new_endcash'] - dslresultdf['old_endcash']
        dslresultdf.to_csv(symbol + str(K_MIN_SAR) + ' finalresult_dsl.csv')

    if calcOwnl:
        ownlresultdf = pd.DataFrame(
            columns=['setname', 'winSwitch', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                     'old_SR',
                     'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                     'maxSingleLoss', 'maxSingleDrawBack'])
        i=0
        for winSwitch in winSwitchList:
            resultList = []
            ownlFolderName = "OnceWinNoLoss" + str(winSwitch * 1000)
            try:
                os.mkdir(ownlFolderName)  # 创建文件夹
            except:
                print "dir already exist!"
            print ("OnceWinNoLoss WinSwitch:%f" % winSwitch)

            setname = str(K_MIN_MACD)+' HopeWin'
            l=ownl.ownlCal(symbol, K_MIN_SAR, setname, bar1m,barxm, winSwitch, nolossThreshhold, slip, ownlFolderName + '\\')

            ownlresultdf.loc[i] = l
            i+=1

        ownlresultdf['cashDelta'] = ownlresultdf['new_endcash'] - ownlresultdf['old_endcash']
        ownlresultdf.to_csv(symbol + str(K_MIN_SAR) + ' finalresult_ownl.csv')

    if calcDslOwnl:
        setname = str(K_MIN_MACD) + ' HopeWin'
        allresultdf = pd.DataFrame(
            columns=['setname', 'dslTarget', 'ownlWinSwtich', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                     'old_SR', 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                     'dslWorknum', 'ownlWorknum', 'dslRetDelta', 'ownlRetDelta'])
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        l = []
        resultList = []
        for stoplossTarget in stoplossList:
            for winSwitch in winSwitchList:
                dslFolderName = "DynamicStopLoss" + str(stoplossTarget * 1000) + '\\'
                ownlFolderName = "OnceWinNoLoss" + str(winSwitch * 1000) + '\\'
                newfolder = ("dsl_%.3f_ownl_%.3f" % (stoplossTarget, winSwitch))
                try:
                    os.mkdir(newfolder)  # 创建文件夹
                except:
                    print newfolder, ' already exist!'
                print ("slTarget:%f ownlSwtich:%f" % (stoplossTarget, winSwitch))

                l.append(pool.apply_async(dslownl.dslAndownlCal,
                                          (symbol, K_MIN_SAR, setname, stoplossTarget, winSwitch, slip, dslFolderName,
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
            allresultdf.loc[i] = res.get()
            i += 1

        allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
        allresultdf.to_csv(symbol + str(K_MIN_SAR) + ' finalresult_dsl_ownl.csv')