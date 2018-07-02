# -*- coding: utf-8 -*-
import multiTargetForward as mtf
import pandas as pd
import DATA_CONSTANTS as DC
import os
import HopeMacdMaWin_Parameter as Parameter
import numpy as np
from datetime import datetime

def getDslMonthParameter(strategyName,dslset,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, endmonth):
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = ' resultDSL_by_tick.csv'
    for dslTarget in dslset:
        rawdatapath = folderpath + "DynamicStopLoss%.1f\\" % (dslTarget * 1000)
        df = mtf.getMonthParameter(strategyName, startmonth, endmonth, symbolinfo, K_MIN, parasetlist, rawdatapath,colslist, resultfilesuffix)
        filenamehead = ("%s%s_%s_%d_%s_parameter_dsl_%.3f" % (rawdatapath,strategyName, symbolinfo.domain_symbol, K_MIN, endmonth,dslTarget))
        df.to_csv(filenamehead + '.csv')

def getownlMonthParameter(strategyName,ownlset,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, endmonth):
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = ' resultOWNL_by_tick.csv'
    for ownlTarget in ownlset:
        rawdatapath = folderpath + "OnceWinNoLoss%.1f\\" % (ownlTarget*1000)
        df = mtf.getMonthParameter(strategyName, startmonth, endmonth, symbolinfo, K_MIN, parasetlist, rawdatapath,colslist, resultfilesuffix)
        filenamehead = ("%s%s_%s_%d_%s_parameter_ownl_%.3f" % (rawdatapath, strategyName,symbolinfo.domain_symbol, K_MIN, endmonth,ownlTarget))
        df.to_csv(filenamehead + '.csv')


def getdsl_ownlMonthParameter(strategyName,dsl_ownl_list,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, endmonth):
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = ' result_dsl_ownl.csv'
    for dsl_ownl in dsl_ownl_list:
        newfolder = ("dsl_%.3f_ownl_%.3f\\" % (dsl_ownl[0], dsl_ownl[1]))
        rawdatapath = folderpath + newfolder  # ！！正常:'\\'，双止损：填上'\\+双止损目标文件夹\\'
        df = mtf.getMonthParameter(strategyName, startmonth, endmonth, symbolinfo, K_MIN, parasetlist, rawdatapath,colslist, resultfilesuffix)
        filenamehead = ("%s%s_%s_%d_%s_parameter_dsl_%.3f_ownl_%.3f" % (rawdatapath,strategyName,symbolinfo.domain_symbol, K_MIN, endmonth,dsl_ownl[0], dsl_ownl[1]))
        df.to_csv(filenamehead + '.csv')

def getmultiStlMonthParameter(strategyName,sltlist,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, endmonth):
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = ' result_multiSLT.csv'
    # 先生成参数列表
    allSltSetList = []  # 这是一个二维的参数列表，每一个元素是一个止损目标的参数dic列表
    for slt in sltlist:
        sltset = []
        for t in slt['paralist']:
            sltset.append({'name': slt['name'],
                           'sltValue': t
                           })
        allSltSetList.append(sltset)
    finalSltSetList = []  # 二维数据，每个一元素是一个多个止损目标的参数dic组合
    for sltpara in allSltSetList[0]:
        finalSltSetList.append([sltpara])
    for i in range(1, len(allSltSetList)):
        tempset = allSltSetList[i]
        newset = []
        for o in finalSltSetList:
            for t in tempset:
                newset.append(o + [t])
        finalSltSetList = newset
    for sltset in finalSltSetList:
        newfolder = ''
        for sltp in sltset:
            newfolder += (sltp['name'] + '_%.3f' % (sltp['sltValue']))
        rawdatapath = folderpath + newfolder + '\\'
        df = mtf.getMonthParameter(strategyName, startmonth, endmonth, symbolinfo, K_MIN, parasetlist, rawdatapath,colslist, resultfilesuffix)
        filenamehead = ("%s%s_%s_%d_%s_parameter_%s" % (rawdatapath,strategyName,symbolinfo.domain_symbol, K_MIN, endmonth,newfolder))
        df.to_csv(filenamehead + '.csv')

if __name__=='__main__':
    # 文件路径
    upperpath = DC.getUpperPath(Parameter.folderLevel)
    resultpath = upperpath + Parameter.resultFolderName

    # 取参数集
    parasetlist = pd.read_csv(resultpath + Parameter.parasetname)
    paranum = parasetlist.shape[0]
    #生成月份列表，取开始月
    newmonth=Parameter.enddate[:7]
    month_n=Parameter.month_n
    monthlist = [datetime.strftime(x, '%Y-%m') for x in
                 list(pd.date_range(start=Parameter.startdate, end=newmonth+'-01', freq='M'))]
    startmonth=monthlist[-month_n]
    # ======================================参数配置==================================================
    strategyName = Parameter.strategyName
    exchange_id = Parameter.exchange_id
    sec_id = Parameter.sec_id
    K_MIN = Parameter.K_MIN
    symbol = '.'.join([exchange_id, sec_id])
    symbolinfo = DC.SymbolInfo(symbol)

    #计算控制开关
    commonForward = Parameter.common_forward
    dsl=Parameter.calcDsl_forward
    ownl=Parameter.calcOwnl_forward
    frsl=Parameter.calsFrsl_forward
    dslownl=Parameter.calcDslOwnl_forward

    multiStl=Parameter.multiSTL_forward

    #文件路径
    foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
    folderpath=resultpath+foldername+'\\'
    os.chdir(folderpath)
    if multiStl:
        sltlist = []
        if dsl:
            dslStep = Parameter.dslStep_forward
            stoplossList = np.arange(Parameter.dslTargetStart_forward, Parameter.dslTargetEnd_forward, dslStep)
            sltlist.append({'name': 'dsl',
                            'paralist': stoplossList})
        if ownl:
            ownlStep = Parameter.ownlStep_forward
            winSwitchList = np.arange(Parameter.ownlTargetStart_forward, Parameter.ownltargetEnd_forward, ownlStep)
            sltlist.append({'name': 'ownl',
                            'paralist': winSwitchList})
        if frsl:
            frslStep = Parameter.frslStep_forward
            fixRateList = np.arange(Parameter.frslTargetStart_forward, Parameter.frslTragetEnd_forward,frslStep)
            sltlist.append({'name': 'frsl',
                            'paralist': fixRateList})
        getmultiStlMonthParameter(strategyName,sltlist,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, newmonth)
    else:
        if commonForward:
            colslist = mtf.getColumnsName(False)
            resultfilesuffix = ' result.csv'
            df = mtf.getMonthParameter(strategyName, startmonth, newmonth, symbolinfo, K_MIN, parasetlist, foldername,colslist, resultfilesuffix)
            filenamehead = ("%s_%s_%d_%s_parameter_common" % (strategyName, symbolinfo.domain_symbol, K_MIN, newmonth))
            df.to_csv(filenamehead + '.csv')

        if dsl:
            dslStep = Parameter.dslStep_forward
            stoplossList = np.arange(Parameter.dslTargetStart_forward, Parameter.dslTargetEnd_forward, dslStep)
            getDslMonthParameter(strategyName,stoplossList,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, newmonth)
        if ownl:
            ownlStep = Parameter.ownlStep_forward
            winSwitchList = np.arange(Parameter.ownlTargetStart_forward, Parameter.ownltargetEnd_forward, ownlStep)
            getownlMonthParameter(strategyName, winSwitchList, symbolinfo, K_MIN, parasetlist, folderpath, startmonth, newmonth)
        if dslownl:
            dsl_ownl_List=Parameter.dsl_ownl_set
            getdsl_ownlMonthParameter(strategyName, dsl_ownl_List, symbolinfo, K_MIN, parasetlist, folderpath, startmonth,newmonth)