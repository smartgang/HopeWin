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
    # ======================================参数配置===================================================
    strategyName=HopeMacdMaWin_Parameter.strategyName
    exchange_id = HopeMacdMaWin_Parameter.exchange_id
    sec_id = HopeMacdMaWin_Parameter.sec_id
    K_MIN = HopeMacdMaWin_Parameter.K_MIN
    startdate = HopeMacdMaWin_Parameter.startdate
    enddate = HopeMacdMaWin_Parameter.enddate
    nextmonth = HopeMacdMaWin_Parameter.nextMonthName
    symbol = '.'.join([exchange_id, sec_id])
    # windowsSet=[1,2,3,4,5,6,9,12,15]
    windowsSet = range(HopeMacdMaWin_Parameter.forwardWinStart, HopeMacdMaWin_Parameter.forwardWinEnd + 1)  # 白区窗口值
    commonForward=HopeMacdMaWin_Parameter.common_forward
    #双止损开关和参数设置
    dslStep=HopeMacdMaWin_Parameter.dslStep_forward
    dsl=HopeMacdMaWin_Parameter.calcDsl_forward
    ownlStep=HopeMacdMaWin_Parameter.ownlStep_forward
    ownl=HopeMacdMaWin_Parameter.calcOwnl_forward
    dslownl=HopeMacdMaWin_Parameter.calcDslOwnl_forward

    dslTargetList=np.arange(HopeMacdMaWin_Parameter.dslTargetStart_close, HopeMacdMaWin_Parameter.dslTargetEnd_close, dslStep)
    ownlTargetlist=np.arange(HopeMacdMaWin_Parameter.ownlTargetStart_close,HopeMacdMaWin_Parameter.ownltargetEnd_close, ownlStep)
    dsl_ownl_List=HopeMacdMaWin_Parameter.dsl_ownl_set

    # ============================================文件路径========================================================
    upperpath = DC.getUpperPath(uppernume=2)
    resultpath = upperpath + "\\Results\\"
    foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
    folderpath = resultpath + foldername + '\\'

    parasetlist = pd.read_csv(resultpath + HopeMacdMaWin_Parameter.parasetname)
    if commonForward:
        colslist = mtf.getColumnsName(False)
        resultfilesuffix = 'result.csv'
        getForward(symbol,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet,colslist,resultfilesuffix)
    if dsl:
        getDslForward(dslTargetList,symbol,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet)
    if ownl:
        getownlForward(ownlTargetlist,symbol,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet)
    if dslownl:
        getdsl_ownlForward(dsl_ownl_List,symbol,K_MIN,parasetlist,folderpath,startdate,enddate,nextmonth,windowsSet)