# -*- coding: utf-8 -*-

import DynamicStopLoss as dsl
import OnceWinNoLoss as ownl
import FixRateStopLoss as frsl
import DslOwnlClose as dslownl
import MultiStopLoss as msl
import DATA_CONSTANTS as DC
import pandas as pd
import os
import numpy as np
import multiprocessing
import datetime
import HopeMacdMaWin_Parameter as Parameter

def bar1mPrepare(bar1m):
    bar1m['longHigh'] = bar1m['high']
    bar1m['shortHigh'] = bar1m['high']
    bar1m['longLow'] = bar1m['low']
    bar1m['shortLow'] = bar1m['low']
    bar1m['highshift1'] = bar1m['high'].shift(1).fillna(0)
    bar1m['lowshift1'] = bar1m['low'].shift(1).fillna(0)
    bar1m.loc[bar1m['open'] < bar1m['close'], 'longHigh'] = bar1m['highshift1']
    bar1m.loc[bar1m['open'] > bar1m['close'], 'shortLow'] = bar1m['lowshift1']

    bar=pd.DataFrame()
    bar['longHigh']=bar1m['longHigh']
    bar['longLow']=bar1m['longLow']
    bar['shortHigh']=bar1m['shortHigh']
    bar['shortLow']=bar1m['shortLow']
    bar['strtime']=bar1m['strtime']
    bar['utc_time']=bar1m['utc_time']
    bar['Unnamed: 0']=bar1m['Unnamed: 0']
    bar['high']=bar1m['high']
    bar['low']=bar1m['low']
    return bar

def getDSL(strategyName,symbolInfo,K_MIN,stoplossList,parasetlist,bar1m,barxm,positionRatio,initialCash,progress=False):
    symbol=symbolInfo.symbol
    pricetick=symbolInfo.getPriceTick()
    allresultdf = pd.DataFrame(
        columns=['setname', 'slTarget', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                 'old_SR',
                 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                 'maxSingleLoss'])
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
                     'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR', 'maxSingleLoss'])
        setnum = 0
        numlist = range(0, paranum, 100)
        numlist.append(paranum)
        for n in range(1, len(numlist)):
            pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
            l = []
            for a in range(numlist[n - 1], numlist[n]):
                setname = parasetlist.ix[a, 'Setname']
                if not progress:
                    l.append(pool.apply_async(dsl.dslCal, (strategyName,
                                                       symbolInfo, K_MIN, setname, bar1m, barxm, pricetick, positionRatio,initialCash,stoplossTarget, dslFolderName + '\\')))
                else:
                    #l.append(dsl.progressDslCal(strategyName,symbolInfo, K_MIN, setname, bar1m, barxm, pricetick,
                    #                                               positionRatio, initialCash, stoplossTarget,
                    #                                               dslFolderName + '\\'))
                    l.append(pool.apply_async(dsl.progressDslCal, (strategyName,
                                                       symbolInfo, K_MIN, setname, bar1m, barxm, pricetick, positionRatio,initialCash,stoplossTarget, dslFolderName + '\\')))
            pool.close()
            pool.join()

            for res in l:
                resultdf.loc[setnum] = res.get()
                allresultdf.loc[allnum] = resultdf.loc[setnum]
                setnum += 1
                allnum += 1
        resultdf['cashDelta'] = resultdf['new_endcash'] - resultdf['old_endcash']
        resultdf.to_csv(dslFolderName + '\\' + strategyName+' '+symbol + str(K_MIN) + ' finalresult_dsl' + str(stoplossTarget) + '.csv')

    allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv(strategyName+' '+symbol + str(K_MIN)+' finalresult_dsl.csv')

def getOwnl(strategyName,symbolInfo,K_MIN,winSwitchList,nolossThreshhold,parasetlist,bar1m,barxm,positionRatio,initialCash,progress=False):
    symbol=symbolInfo.symbol
    ownlallresultdf = pd.DataFrame(
        columns=['setname', 'winSwitch', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                 'old_SR',
                 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR','maxSingleLoss'])
    allnum=0
    paranum=parasetlist.shape[0]
    for winSwitch in winSwitchList:
        ownlFolderName = "OnceWinNoLoss" + str(winSwitch * 1000)
        try:
            os.mkdir(ownlFolderName)  # 创建文件夹
        except:
            #print "dir already exist!"
            pass
        print ("OnceWinNoLoss WinSwitch:%f" % winSwitch)

        ownlresultdf = pd.DataFrame(
            columns=['setname', 'winSwitch', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                     'old_SR', 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR','maxSingleLoss'])

        setnum = 0
        numlist = range(0, paranum, 100)
        numlist.append(paranum)
        for n in range(1, len(numlist)):
            pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
            l = []
            for a in range(numlist[n - 1], numlist[n]):
                setname = parasetlist.ix[a, 'Setname']
                if not progress:
                    l.append(pool.apply_async(ownl.ownlCal,
                                              (strategyName,symbolInfo, K_MIN, setname, bar1m, barxm, winSwitch, nolossThreshhold, positionRatio,initialCash,
                                               ownlFolderName + '\\')))
                else:
                    #l.append(ownl.progressOwnlCal(strategyName, symbolInfo, K_MIN, setname, bar1m, barxm, winSwitch,
                    #                           nolossThreshhold, positionRatio, initialCash,
                    #                           ownlFolderName + '\\'))
                    l.append(pool.apply_async(ownl.progressOwnlCal,
                                              (strategyName,symbolInfo, K_MIN, setname, bar1m, barxm, winSwitch, nolossThreshhold, positionRatio,initialCash,
                                               ownlFolderName + '\\')))
            pool.close()
            pool.join()

            for res in l:
                ownlresultdf.loc[setnum] = res.get()
                ownlallresultdf.loc[allnum] = ownlresultdf.loc[setnum]
                setnum += 1
                allnum += 1
        ownlresultdf['cashDelta'] = ownlresultdf['new_endcash'] - ownlresultdf['old_endcash']
        ownlresultdf.to_csv(ownlFolderName + '\\' +strategyName+' '+ symbol + str(K_MIN) + ' finalresult_ownl' + str(winSwitch) + '.csv')

    ownlallresultdf['cashDelta'] = ownlallresultdf['new_endcash'] - ownlallresultdf['old_endcash']
    ownlallresultdf.to_csv(strategyName+' '+symbol + str(K_MIN)+' finalresult_ownl.csv')


def getFRSL(strategyName,symbolInfo,K_MIN,fixRateList,parasetlist,bar1m,barxm,positionRatio,initialCash,progress=False):
    symbol=symbolInfo.symbol
    allresultdf = pd.DataFrame(
        columns=['setname', 'fixRate', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                 'old_SR',
                 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                 'maxSingleLoss'])
    allnum = 0
    paranum=parasetlist.shape[0]
    for fixRateTarget in fixRateList:

        folderName = "FixRateStopLoss" + str(fixRateTarget * 1000)
        try:
            os.mkdir(folderName)  # 创建文件夹
        except:
            #print 'folder already exist'
            pass
        print ("fixRateTarget:%f" % fixRateTarget)

        resultdf = pd.DataFrame(
            columns=['setname', 'fixRate', 'worknum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                     'old_SR',
                     'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR', 'maxSingleLoss'])
        setnum = 0
        numlist = range(0, paranum, 100)
        numlist.append(paranum)
        for n in range(1, len(numlist)):
            pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
            l = []
            for a in range(numlist[n - 1], numlist[n]):
                setname = parasetlist.ix[a, 'Setname']
                if not progress:
                    #l.append(frsl.frslCal(strategyName,
                    #                                       symbolInfo, K_MIN, setname, bar1m, barxm, fixRateTarget, positionRatio,initialCash, folderName + '\\'))
                    l.append(pool.apply_async(frsl.frslCal, (strategyName,
                                                           symbolInfo, K_MIN, setname, bar1m, barxm, fixRateTarget, positionRatio,initialCash, folderName + '\\')))
                else:
                    l.append(pool.apply_async(frsl.progressFrslCal, (strategyName,
                                                           symbolInfo, K_MIN, setname, bar1m, barxm, fixRateTarget, positionRatio,initialCash, folderName + '\\')))
            pool.close()
            pool.join()

            for res in l:
                resultdf.loc[setnum] = res.get()
                allresultdf.loc[allnum] = resultdf.loc[setnum]
                setnum += 1
                allnum += 1
        resultdf['cashDelta'] = resultdf['new_endcash'] - resultdf['old_endcash']
        resultdf.to_csv(folderName + '\\' + strategyName+' '+symbol + str(K_MIN) + ' finalresult_frsl' + str(fixRateTarget) + '.csv')

    allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv(strategyName+' '+symbol + str(K_MIN)+' finalresult_frsl.csv')

def getDslOwnl(strategyName,symbolInfo,K_MIN,parasetlist,stoplossList,winSwitchList,positionRatio,initialCash):
    symbol=symbolInfo.symbol
    allresultdf = pd.DataFrame(
        columns=['setname', 'dslTarget', 'ownlWinSwtich', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                 'old_SR', 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                 'dslWorknum', 'ownlWorknum', 'dslRetDelta', 'ownlRetDelta'])
    allnum=0
    paranum=parasetlist.shape[0]
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
                                                  (strategyName,symbolInfo, K_MIN,setname, stoplossTarget, winSwitch, positionRatio,initialCash,dslFolderName,
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
            resultfilename = ("%s %s%d finalresult_dsl%.3f_ownl%.3f.csv" % (strategyName,symbol, K_MIN, stoplossTarget, winSwitch))
            resultdf.to_csv(newfolder + '\\' + resultfilename)

    allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv(strategyName+' '+symbol + str(K_MIN)+ ' finalresult_dsl_ownl.csv')

def getMultiSLT(strategyName,symbolInfo,K_MIN,parasetlist,sltlist,positionRatio,initialCash):
    '''
    计算多个止损策略结合回测的结果
    :param strategyName:
    :param symbolInfo:
    :param K_MIN:
    :param parasetlist:
    :param sltlist:
    :param positionRatio:
    :param initialCash:
    :return:
    '''
    symbol=symbolInfo.symbol
    allresultdf = pd.DataFrame(
        columns=['setname','slt', 'slWorkNum', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                 'old_SR', 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',])
    allnum=0
    paranum=parasetlist.shape[0]
    #先生成参数列表
    allSltSetList=[] #这是一个二维的参数列表，每一个元素是一个止损目标的参数dic列表
    for slt in sltlist:
        sltset=[]
        for t in slt['paralist']:
            sltset.append({'name':slt['name'],
                           'sltValue':t,
                            'folder':slt['folderPrefix']+str(t * 1000) + '\\',
                            'fileSuffix':slt['fileSuffix']
                           })
        allSltSetList.append(sltset)
    finalSltSetList=[]#二维数据，每个一元素是一个多个止损目标的参数dic组合
    for sltpara in allSltSetList[0]:
        finalSltSetList.append([sltpara])
    for i in range(1,len(allSltSetList)):
        tempset = allSltSetList[i]
        newset = []
        for o in finalSltSetList:
            for t in tempset:
                newset.append(o + [t])
        finalSltSetList = newset
    print finalSltSetList

    for sltset in finalSltSetList:
        newfolder=''
        for sltp in sltset:
            newfolder += (sltp['name']+'_%.3f' %(sltp['sltValue']))
        try:
            os.mkdir(newfolder)  # 创建文件夹
        except:
            pass
        print (newfolder)
        pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
        l = []
        for sn in range(0, paranum):
            setname = parasetlist.ix[sn, 'Setname']
            #l.append(msl.multiStopLosslCal(strategyName, symbolInfo, K_MIN, setname, sltset, positionRatio, initialCash,
            #                           newfolder + '\\'))
            l.append(pool.apply_async(msl.multiStopLosslCal,
                                              (strategyName,symbolInfo, K_MIN,setname, sltset, positionRatio,initialCash,newfolder )))
        pool.close()
        pool.join()

        resultdf = pd.DataFrame(
            columns=['setname','slt', 'slWorkNum', 'old_endcash', 'old_Annual', 'old_Sharpe','old_Drawback',
                     'old_SR', 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR'])
        i = 0
        for res in l:
            resultdf.loc[i] = res.get()
            allresultdf.loc[allnum] = resultdf.loc[i]
            i += 1
            allnum+=1
        resultfilename = ("%s %s%d finalresult_multiSLT_%s.csv" % (strategyName,symbol, K_MIN, newfolder))
        resultdf.to_csv(newfolder + '\\' + resultfilename)

    allresultname=''
    for slt in sltlist:
        allresultname += slt['name']
    allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    #allresultdf.to_csv(strategyName+' '+symbol + str(K_MIN)+ ' finalresult_multiSLT.csv')
    allresultdf.to_csv("%s %s%d finalresult_multiSLT_%s.csv"%(strategyName,symbol,K_MIN,allresultname))
    pass

if __name__=='__main__':
    # 文件路径
    upperpath = DC.getUpperPath(Parameter.folderLevel)
    resultpath = upperpath + Parameter.resultFolderName

    # 取参数集
    parasetlist = pd.read_csv(resultpath + Parameter.parasetname)
    paranum = parasetlist.shape[0]

    # 参数设置
    strategyParameterSet = []
    if not Parameter.symbol_KMIN_opt_swtich:
        # 单品种单周期模式
        paradic = {
            'strategyName': Parameter.strategyName,
            'exchange_id': Parameter.exchange_id,
            'sec_id': Parameter.sec_id,
            'K_MIN': Parameter.K_MIN,
            'startdate': Parameter.startdate,
            'enddate': Parameter.enddate,
            'positionRatio':Parameter.positionRatio,
            'initialCash' : Parameter.initialCash,
            'progress':Parameter.progress_close,
            'calcDsl': Parameter.calcDsl_close,
            'calcOwnl': Parameter.calcOwnl_close,
            'calcFrsl': Parameter.calcFrsl_close,
            'calcMultiSLT':Parameter.calcMultiSLT_close,
            'calcDslOwnl': Parameter.calcDslOwnl_close,
            'dslStep':Parameter.dslStep_close,
            'dslTargetStart':Parameter.dslTargetStart_close,
            'dslTargetEnd':Parameter.dslTargetEnd_close,
            'ownlStep' : Parameter.ownlStep_close,
            'ownlTargetStart': Parameter.ownlTargetStart_close,
            'ownltargetEnd': Parameter.ownltargetEnd_close,
            'nolossThreshhold':Parameter.nolossThreshhold_close,
            'frslStep': Parameter.frslStep_close,
            'frslTargetStart':Parameter.frslTargetStart_close,
            'frslTargetEnd': Parameter.frslTragetEnd_close
        }
        strategyParameterSet.append(paradic)
    else:
        # 多品种多周期模式
        symbolset = pd.read_excel(resultpath + Parameter.stoploss_set_filename,index_col='No')
        symbolsetNum = symbolset.shape[0]
        for i in range(symbolsetNum):
            exchangeid = symbolset.ix[i, 'exchange_id']
            secid = symbolset.ix[i, 'sec_id']
            strategyParameterSet.append({
                'strategyName': symbolset.ix[i, 'strategyName'],
                'exchange_id': exchangeid,
                'sec_id': secid,
                'K_MIN': symbolset.ix[i, 'K_MIN'],
                'startdate': symbolset.ix[i, 'startdate'],
                'enddate': symbolset.ix[i, 'enddate'],
                'positionRatio' : Parameter.positionRatio,
                'initialCash' : Parameter.initialCash,
                'progress':symbolset.ix[i,'progress'],
                'calcDsl': symbolset.ix[i, 'calcDsl'],
                'calcOwnl': symbolset.ix[i, 'calcOwnl'],
                'calcFrsl': symbolset.ix[i, 'calcFrsl'],
                'calcMultiSLT': symbolset.ix[i,'calcMultiSLT'],
                'calcDslOwnl': symbolset.ix[i, 'calcDslOwnl'],
                'dslStep': symbolset.ix[i, 'dslStep'],
                'dslTargetStart': symbolset.ix[i, 'dslTargetStart'],
                'dslTargetEnd': symbolset.ix[i, 'dslTargetEnd'],
                'ownlStep': symbolset.ix[i, 'ownlStep'],
                'ownlTargetStart': symbolset.ix[i, 'ownlTargetStart'],
                'ownltargetEnd': symbolset.ix[i, 'ownltargetEnd'],
                'nolossThreshhold': symbolset.ix[i, 'nolossThreshhold'],
                'frslStep': symbolset.ix[i, 'frslStep'],
                'frslTargetStart': symbolset.ix[i, 'frslTargetStart'],
                'frslTargetEnd': symbolset.ix[i, 'frslTargetEnd']
            }
            )

    for strategyParameter in strategyParameterSet:

        strategyName = strategyParameter['strategyName']
        exchange_id = strategyParameter['exchange_id']
        sec_id = strategyParameter['sec_id']
        K_MIN = strategyParameter['K_MIN']
        startdate = strategyParameter['startdate']
        enddate = strategyParameter['enddate']
        symbol = '.'.join([exchange_id, sec_id])

        positionRatio = strategyParameter['positionRatio']
        initialCash = strategyParameter['initialCash']

        symbolinfo = DC.SymbolInfo(symbol)
        slip = DC.getSlip(symbol)
        pricetick = DC.getPriceTick(symbol)

        #计算控制开关
        progress=strategyParameter['progress']
        calcDsl=strategyParameter['calcDsl']
        calcOwnl=strategyParameter['calcOwnl']
        calcFrsl=strategyParameter['calcFrsl']
        calcMultiSLT=strategyParameter['calcMultiSLT']
        calcDslOwnl=strategyParameter['calcDslOwnl']

        #优化参数
        dslStep = strategyParameter['dslStep']
        stoplossList = np.arange(strategyParameter['dslTargetStart'], strategyParameter['dslTargetEnd'], dslStep)
        #stoplossList=[-0.022]
        ownlStep=strategyParameter['ownlStep']
        winSwitchList = np.arange(strategyParameter['ownlTargetStart'], strategyParameter['ownltargetEnd'], ownlStep)
        #winSwitchList=[0.009]
        nolossThreshhold = strategyParameter['nolossThreshhold'] * pricetick
        frslStep=strategyParameter['frslStep']
        fixRateList=np.arange(strategyParameter['frslTargetStart'], strategyParameter['frslTargetEnd'], frslStep)

        #文件路径
        foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
        oprresultpath=resultpath+foldername+'\\'
        os.chdir(oprresultpath)

        #原始数据处理
        bar1m=DC.getBarData(symbol=symbol,K_MIN=60,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
        barxm=DC.getBarData(symbol=symbol,K_MIN=K_MIN,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
        #bar1m计算longHigh,longLow,shortHigh,shortLow
        bar1m=bar1mPrepare(bar1m)

        if calcMultiSLT:
            sltlist=[]
            if calcDsl:
                sltlist.append({'name':'dsl',
                                'paralist':stoplossList,
                                'folderPrefix':'DynamicStopLoss',
                                'fileSuffix':'resultDSL_by_tick.csv'})
            if calcOwnl:
                sltlist.append({'name':'ownl',
                                'paralist':winSwitchList,
                                'folderPrefix':'OnceWinNoLoss',
                                'fileSuffix':'resultOWNL_by_tick.csv'})
            if calcFrsl:
                sltlist.append({'name':'frsl',
                                'paralist':fixRateList,
                                'folderPrefix':'FixRateStopLoss',
                                'fileSuffix':'resultFRSL_by_tick.csv'})
            getMultiSLT(strategyName,symbolinfo,K_MIN,parasetlist,sltlist,positionRatio,initialCash)
        else:
            if calcDsl:
                getDSL(strategyName,symbolinfo, K_MIN, stoplossList, parasetlist, bar1m,barxm,positionRatio,initialCash,progress)

            if calcOwnl:
                getOwnl(strategyName,symbolinfo,K_MIN,winSwitchList,nolossThreshhold,parasetlist,bar1m,barxm,positionRatio,initialCash,progress)

            if calcFrsl:
                getFRSL(strategyName,symbolinfo,K_MIN,fixRateList,parasetlist,bar1m,barxm,positionRatio,initialCash,progress)

            if calcDslOwnl:
                getDslOwnl(strategyName,symbolinfo,K_MIN,parasetlist,stoplossList,winSwitchList,positionRatio,initialCash)
