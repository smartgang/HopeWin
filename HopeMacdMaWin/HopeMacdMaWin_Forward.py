# -*- coding: utf-8 -*-
import multiTargetForward as mtf
from datetime import datetime
import pandas as pd
import DATA_CONSTANTS as DC
import os
import multiprocessing
import HopeMacdMaWin_Parameter
import numpy as np

def getForward(symbol,K_MIN,parasetlist,rawdatapath,startdate,enddate,nextmonth,windowsSet,colslist,resultfilesuffix):
    forwordresultpath = rawdatapath + '\\ForwardResults\\'
    forwardrankpath = rawdatapath + '\\ForwardRank\\'
    monthlist = [datetime.strftime(x, '%b-%y') for x in list(pd.date_range(start=startdate, end=enddate, freq='M'))]
    monthlist.append(nextmonth)
    os.chdir(rawdatapath)
    try:
        os.mkdir('ForwardResults')
    except:
        print 'ForwardResults already exist!'
    try:
        os.mkdir('ForwardRank')
    except:
        print 'ForwardRank already exist!'
    try:
        os.mkdir('ForwardOprAnalyze')
    except:
        print 'ForwardOprAnalyze already exist!'

    starttime = datetime.now()
    print starttime
    # 多进程优化，启动一个对应CPU核心数量的进程池
    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    l = []
    for whiteWindows in windowsSet:
        # l.append(mtf.runPara(whiteWindows, symbol, K_MIN, parasetlist, monthlist, rawdatapath, forwordresultpath, forwardrankpath, colslist, resultfilesuffix))
        l.append(pool.apply_async(mtf.runPara, (
        whiteWindows, symbol, K_MIN, parasetlist, monthlist, rawdatapath, forwordresultpath, forwardrankpath, colslist,
        resultfilesuffix)))
    pool.close()
    pool.join()

    mtf.calGrayResult(symbol, K_MIN, windowsSet, forwardrankpath, rawdatapath)
    mtf.calOprResult(rawdatapath, symbol, K_MIN, nextmonth, columns=colslist, resultfilesuffix=resultfilesuffix)
    endtime = datetime.now()
    print starttime
    print endtime

def getDslForward(dslset,symbol, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet):
    print ('DSL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultDSL_by_tick.csv'
    for dslTarget in dslset:
        rawdatapath = folderpath + "DynamicStopLoss" + str(dslTarget * 1000) + '\\'
        getForward(symbol, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist,resultfilesuffix)
    print ('DSL forward finished!')

def getownlForward(ownlset,symbol, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet):
    print ('OWNL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultOWNL_by_tick.csv'
    for ownlTarget in ownlset:
        rawdatapath = folderpath + "OnceWinNoLoss" + str(ownlTarget*1000) + '\\'
        getForward(symbol, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist,resultfilesuffix)
    print ('OWNL forward finished!')

def getdsl_ownlForward(dsl_ownl_list,symbol, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet):
    print ('DSL_OWNL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'result_dsl_ownl.csv'
    for dsl_ownl in dsl_ownl_list:
        newfolder = ("dsl_%.3f_ownl_%.3f\\" % (dsl_ownl[0], dsl_ownl[1]))
        rawdatapath = folderpath + newfolder  # ！！正常:'\\'，双止损：填上'\\+双止损目标文件夹\\'
        getForward(symbol, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, resultfilesuffix)
    print ('DSL_OWNL forward finished!')


if __name__=='__main__':
    # 文件路径
    upperpath = DC.getUpperPath(HopeMacdMaWin_Parameter.folderLevel)
    resultpath = upperpath + HopeMacdMaWin_Parameter.resultFolderName

    # 取参数集
    parasetlist = pd.read_csv(resultpath + HopeMacdMaWin_Parameter.parasetname)
    paranum = parasetlist.shape[0]
    windowsSet = range(HopeMacdMaWin_Parameter.forwardWinStart, HopeMacdMaWin_Parameter.forwardWinEnd + 1)  # 白区窗口值
    # ======================================参数配置===================================================
    strategyParameterSet = []
    if not HopeMacdMaWin_Parameter.symbol_KMIN_opt_swtich:
        # 单品种单周期模式
        paradic = {
            'strategyName': HopeMacdMaWin_Parameter.strategyName,
            'exchange_id': HopeMacdMaWin_Parameter.exchange_id,
            'sec_id': HopeMacdMaWin_Parameter.sec_id,
            'K_MIN': HopeMacdMaWin_Parameter.K_MIN,
            'startdate': HopeMacdMaWin_Parameter.startdate,
            'enddate': HopeMacdMaWin_Parameter.enddate,
            'nextmonth':HopeMacdMaWin_Parameter.nextMonthName,
            'commonForward': HopeMacdMaWin_Parameter.common_forward,
            'calcDsl': HopeMacdMaWin_Parameter.calcDsl_forward,
            'calcOwnl': HopeMacdMaWin_Parameter.calcOwnl_forward,
            'calcDslOwnl': HopeMacdMaWin_Parameter.calcDslOwnl_forward,
            'dslStep':HopeMacdMaWin_Parameter.dslStep_forward,
            'dslTargetStart':HopeMacdMaWin_Parameter.dslTargetStart_forward,
            'dslTargetEnd':HopeMacdMaWin_Parameter.dslTargetEnd_forward,
            'ownlStep' : HopeMacdMaWin_Parameter.ownlStep_forward,
            'ownlTargetStart': HopeMacdMaWin_Parameter.ownlTargetStart_forward,
            'ownltargetEnd': HopeMacdMaWin_Parameter.ownltargetEnd_forward,
            'dsl_own_set': HopeMacdMaWin_Parameter.dsl_ownl_set
        }
        strategyParameterSet.append(paradic)
    else:
        # 多品种多周期模式
        symbolset = pd.read_excel(resultpath + HopeMacdMaWin_Parameter.forward_set_filename,index_col='No')
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
                'nextmonth':symbolset.ix[i,'nextmonth'],
                'commonForward':symbolset.ix[i,'commonForward'],
                'calcDsl': symbolset.ix[i, 'calcDsl'],
                'calcOwnl': symbolset.ix[i, 'calcOwnl'],
                'calcDslOwnl': symbolset.ix[i, 'calcDslOwnl'],
                'dslStep': symbolset.ix[i, 'dslStep'],
                'dslTargetStart': symbolset.ix[i, 'dslTargetStart'],
                'dslTargetEnd': symbolset.ix[i, 'dslTargetEnd'],
                'ownlStep': symbolset.ix[i, 'ownlStep'],
                'ownlTargetStart': symbolset.ix[i, 'ownlTargetStart'],
                'ownltargetEnd': symbolset.ix[i, 'ownltargetEnd'],
                'dslownl_dsl': symbolset.ix[i, 'dslownl_dsl'],
                'dslownl_ownl':symbolset.ix[i,'dslownl_ownl']
            }
            )

    for strategyParameter in strategyParameterSet:

        strategyName = strategyParameter['strategyName']
        exchange_id = strategyParameter['exchange_id']
        sec_id = strategyParameter['sec_id']
        K_MIN = strategyParameter['K_MIN']
        startdate = strategyParameter['startdate']
        enddate = strategyParameter['enddate']
        nextmonth = strategyParameter['nextmonth']
        symbol = '.'.join([exchange_id, sec_id])

        symbolinfo = DC.SymbolInfo(symbol)
        slip = DC.getSlip(symbol)
        pricetick = DC.getPriceTick(symbol)

        #计算控制开关
        commonForward = strategyParameter['commonForward']
        dsl=strategyParameter['calcDsl']
        ownl=strategyParameter['calcOwnl']
        dslownl=strategyParameter['calcDslOwnl']

        #文件路径
        foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
        folderpath=resultpath+foldername+'\\'
        os.chdir(folderpath)

        parasetlist = pd.read_csv(resultpath + HopeMacdMaWin_Parameter.parasetname)

        if commonForward:
            colslist = mtf.getColumnsName(False)
            resultfilesuffix = 'result.csv'
            getForward(symbol,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet,colslist,resultfilesuffix)
        if dsl:
            dslStep = strategyParameter['dslStep']
            stoplossList = np.arange(strategyParameter['dslTargetStart'], strategyParameter['dslTargetEnd'], dslStep)
            getDslForward(stoplossList,symbol,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet)
        if ownl:
            ownlStep = strategyParameter['ownlStep']
            winSwitchList = np.arange(strategyParameter['ownlTargetStart'], strategyParameter['ownltargetEnd'],
                                      ownlStep)
            getownlForward(winSwitchList,symbol,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet)
        if dslownl:
            if not HopeMacdMaWin_Parameter.symbol_KMIN_opt_swtich:
                dsl_ownl_List=strategyParameter['dsl_own_set']
            else:
                dsl_ownl_List = [[strategyParameter['dslownl_dsl'],strategyParameter['dslownl_ownl']]]
            getdsl_ownlForward(dsl_ownl_List,symbol,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet)