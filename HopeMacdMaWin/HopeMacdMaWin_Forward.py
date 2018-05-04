# -*- coding: utf-8 -*-
import multiTargetForward as mtf
from datetime import datetime
import pandas as pd
import DATA_CONSTANTS as DC
import os
import multiprocessing
import HopeMacdMaWin_Parameter as Parameter
import numpy as np

def getForward(strategyName,symbolinfo,K_MIN,parasetlist,rawdatapath,startdate,enddate,nextmonth,windowsSet,colslist,positionRatio,initialCash,resultfilesuffix):
    symbol=symbolinfo.symbol
    forwordresultpath = rawdatapath + '\\ForwardResults\\'
    forwardrankpath = rawdatapath + '\\ForwardRank\\'
    monthlist = [datetime.strftime(x, '%Y-%m') for x in list(pd.date_range(start=startdate, end=enddate, freq='M'))]
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
        #l.append(mtf.runPara(strategyName,whiteWindows, symbolinfo, K_MIN, parasetlist, monthlist, rawdatapath, forwordresultpath, forwardrankpath, colslist, resultfilesuffix))
        l.append(pool.apply_async(mtf.runPara, (strategyName,whiteWindows, symbolinfo, K_MIN, parasetlist, monthlist, rawdatapath, forwordresultpath, forwardrankpath, colslist, resultfilesuffix)))
    pool.close()
    pool.join()

    mtf.calGrayResult(strategyName,symbol, K_MIN, windowsSet, forwardrankpath, rawdatapath)
    mtf.calOprResult(strategyName,rawdatapath, symbolinfo, K_MIN, nextmonth, columns=colslist, positionRatio=positionRatio,initialCash=initialCash,resultfilesuffix=resultfilesuffix)
    endtime = datetime.now()
    print starttime
    print endtime

def getDslForward(strategyName,dslset,symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet,positionRatio,initialCash):
    print ('DSL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultDSL_by_tick.csv'
    for dslTarget in dslset:
        rawdatapath = folderpath + "DynamicStopLoss" + str(dslTarget * 1000) + '\\'
        getForward(strategyName,symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist,positionRatio,initialCash,resultfilesuffix)
    print ('DSL forward finished!')

def getownlForward(strategyName,ownlset,symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet,positionRatio,initialCash):
    print ('OWNL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultOWNL_by_tick.csv'
    for ownlTarget in ownlset:
        rawdatapath = folderpath + "OnceWinNoLoss" + str(ownlTarget*1000) + '\\'
        getForward(strategyName,symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist,positionRatio,initialCash,resultfilesuffix)
    print ('OWNL forward finished!')

def getdsl_ownlForward(strategyName,dsl_ownl_list,symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet,positionRatio,initialCash):
    print ('DSL_OWNL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'result_dsl_ownl.csv'
    for dsl_ownl in dsl_ownl_list:
        newfolder = ("dsl_%.3f_ownl_%.3f\\" % (dsl_ownl[0], dsl_ownl[1]))
        rawdatapath = folderpath + newfolder  # ！！正常:'\\'，双止损：填上'\\+双止损目标文件夹\\'
        getForward(strategyName,symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, positionRatio,initialCash,resultfilesuffix)
    print ('DSL_OWNL forward finished!')


if __name__=='__main__':
    # 文件路径
    upperpath = DC.getUpperPath(Parameter.folderLevel)
    resultpath = upperpath + Parameter.resultFolderName

    # 取参数集
    parasetlist = pd.read_csv(resultpath + Parameter.parasetname)
    paranum = parasetlist.shape[0]
    windowsSet = range(Parameter.forwardWinStart, Parameter.forwardWinEnd + 1)  # 白区窗口值
    # ======================================参数配置===================================================
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
            'positionRatio': Parameter.positionRatio,
            'initialCash' : Parameter.initialCash,
            #'nextmonth':Parameter.nextMonthName,
            'commonForward': Parameter.common_forward,
            'calcDsl': Parameter.calcDsl_forward,
            'calcOwnl': Parameter.calcOwnl_forward,
            'calcDslOwnl': Parameter.calcDslOwnl_forward,
            'dslStep':Parameter.dslStep_forward,
            'dslTargetStart':Parameter.dslTargetStart_forward,
            'dslTargetEnd':Parameter.dslTargetEnd_forward,
            'ownlStep' : Parameter.ownlStep_forward,
            'ownlTargetStart': Parameter.ownlTargetStart_forward,
            'ownltargetEnd': Parameter.ownltargetEnd_forward,
            'dsl_own_set': Parameter.dsl_ownl_set
        }
        strategyParameterSet.append(paradic)
    else:
        # 多品种多周期模式
        symbolset = pd.read_excel(resultpath + Parameter.forward_set_filename,index_col='No')
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
                'positionRatio':Parameter.positionRatio,
                'initialCash':Parameter.initialCash,
                #'nextmonth':symbolset.ix[i,'nextmonth'],
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
        #nextmonth = strategyParameter['nextmonth']
        nextmonth = enddate[:7]
        symbol = '.'.join([exchange_id, sec_id])

        positionRatio=strategyParameter['positionRatio']
        initialCash = strategyParameter['initialCash']

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

        parasetlist = pd.read_csv(resultpath + Parameter.parasetname)

        if commonForward:
            colslist = mtf.getColumnsName(False)
            resultfilesuffix = 'result.csv'
            getForward(strategyName,symbolinfo,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet,colslist,positionRatio,initialCash,resultfilesuffix)
        if dsl:
            dslStep = strategyParameter['dslStep']
            stoplossList = np.arange(strategyParameter['dslTargetStart'], strategyParameter['dslTargetEnd'], dslStep)
            getDslForward(strategyName,stoplossList,symbolinfo,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet,positionRatio,initialCash)
        if ownl:
            ownlStep = strategyParameter['ownlStep']
            winSwitchList = np.arange(strategyParameter['ownlTargetStart'], strategyParameter['ownltargetEnd'],
                                      ownlStep)
            getownlForward(strategyName,winSwitchList,symbolinfo,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet,positionRatio,initialCash)
        if dslownl:
            if not Parameter.symbol_KMIN_opt_swtich:
                dsl_ownl_List=strategyParameter['dsl_own_set']
            else:
                dsl_ownl_List = [[strategyParameter['dslownl_dsl'],strategyParameter['dslownl_ownl']]]
            getdsl_ownlForward(strategyName,dsl_ownl_List,symbolinfo,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet,positionRatio,initialCash)