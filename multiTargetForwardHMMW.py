# -*- coding: utf-8 -*-
import multiTargetForward as mtf
from datetime import datetime
import pandas as pd
import DATA_CONSTANTS as DC
import os
import multiprocessing

if __name__=='__main__':
    # ======================================参数配置===================================================
    strategyName='Hope_MACD_MA'
    exchange_id = 'DCE'
    sec_id = 'I'
    K_MIN = 900
    symbol = '.'.join([exchange_id, sec_id])
    startdate = '2016-01-01'
    enddate = '2017-12-31'
    nextmonth = 'Jan-18'
    # windowsSet=[1,2,3,4,5,6,9,12,15]
    windowsSet = range(1, 13)  # 白区窗口值
    #双止损开关和参数设置
    dsl=True
    ownl=False
    dslownl=False
    dslTarget=-0.022
    ownlTarget=0.009
    #=============================================================================================================
    #newresult = True #！！正常:False，双止损:True
    if dsl:
        resultfilesuffix = 'resultDSL_by_tick.csv'  # 前面不带空格,正常:result.csv,dsl:resultDSL_by_tick.csv,ownl:resultOWNL_by_tick.csv
    elif ownl:
        resultfilesuffix = 'resultOWNL_by_tick.csv'  # 前面不带空格,正常:result.csv,dsl:resultDSL_by_tick.csv,ownl:resultOWNL_by_tick.csv
    elif dslownl:
        resultfilesuffix= 'result_dsl_ownl.csv'
    else:
        resultfilesuffix='result.csv'
    monthlyretrsuffix = 'monthly_retr_new.csv'  # 前面不带下划线,正常:monthly_retr.csv,双止损:monthly_retr_new.csv
    colslist = mtf.getColumnsName(dsl or ownl or dslownl)

    # ============================================文件路径========================================================
    upperpath = DC.getUpperPath(uppernume=1)
    resultpath = upperpath + "\\Results\\"
    foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
    #rawdatapath = resultpath + foldername + '\\'

    parasetlist = pd.read_csv(resultpath + 'MACDParameterSet1.csv')

    if dsl:
        rawdatapath = resultpath + foldername + "\\DynamicStopLoss" + str(dslTarget*1000)+'\\'  # ！！正常:'\\'，双止损：填上'\\+双止损目标文件夹\\'
    elif ownl:
        rawdatapath = resultpath + foldername + "\\OnceWinNoLoss" + str(ownlTarget*1000)+'\\'  # ！！正常:'\\'，双止损：填上'\\+双止损目标文件夹\\'
    elif dslownl:
        newfolder = ("\\dsl_%.3f_ownl_%.3f\\" % (dslTarget, ownlTarget))
        rawdatapath = resultpath + foldername + newfolder  # ！！正常:'\\'，双止损：填上'\\+双止损目标文件夹\\'
    else:
        rawdatapath = resultpath + foldername+'\\'

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
        #l.append(mtf.runPara(whiteWindows, symbol, K_MIN, parasetlist, monthlist, rawdatapath, forwordresultpath, forwardrankpath, colslist, resultfilesuffix))
        l.append(pool.apply_async(mtf.runPara, (whiteWindows, symbol, K_MIN, parasetlist, monthlist, rawdatapath, forwordresultpath, forwardrankpath, colslist,resultfilesuffix)))
    pool.close()
    pool.join()

    mtf.calGrayResult(symbol, K_MIN, windowsSet, forwardrankpath, rawdatapath, monthlyfilesuffix=monthlyretrsuffix)

    mtf.calOprResult(rawdatapath, symbol, K_MIN, nextmonth, columns=colslist, resultfilesuffix=resultfilesuffix)
    endtime = datetime.now()
    print starttime
    print endtime