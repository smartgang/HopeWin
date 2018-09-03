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
        rawdatapath = folderpath + "DynamicStopLoss%s\\" % dslTarget['para_name']
        df = mtf.getMonthParameter(strategyName, startmonth, endmonth, symbolinfo, K_MIN, parasetlist, rawdatapath,colslist, resultfilesuffix)
        filenamehead = ("%s%s_%s_%d_%s_parameter_dsl_%s" % (rawdatapath,strategyName, symbolinfo.domain_symbol, K_MIN, endmonth,dslTarget['para_name']))
        df.to_csv(filenamehead + '.csv')


def getownlMonthParameter(strategyName,ownlset,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, endmonth):
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = ' resultOWNL_by_tick.csv'
    for ownlTarget in ownlset:
        rawdatapath = folderpath + "OnceWinNoLoss%s\\" % ownlTarget['para_name']
        df = mtf.getMonthParameter(strategyName, startmonth, endmonth, symbolinfo, K_MIN, parasetlist, rawdatapath,colslist, resultfilesuffix)
        filenamehead = ("%s%s_%s_%d_%s_parameter_ownl_%s" % (rawdatapath, strategyName,symbolinfo.domain_symbol, K_MIN, endmonth,ownlTarget['para_name']))
        df.to_csv(filenamehead + '.csv')


def getfrslMonthParameter(strategyName,frsl_dic_list,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, endmonth):
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultFRSL_by_tick.csv'
    for frsl_dic in frsl_dic_list:
        rawdatapath = folderpath + "FixRateStopLoss%s\\" % frsl_dic['para_name']
        df = mtf.getMonthParameter(strategyName, startmonth, endmonth, symbolinfo, K_MIN, parasetlist, rawdatapath,colslist, resultfilesuffix)
        filenamehead = ("%s%s_%s_%d_%s_parameter_frsl_%s" % (rawdatapath, strategyName,symbolinfo.domain_symbol, K_MIN, endmonth, frsl_dic['para_name']))
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

    pricetick = symbolinfo.getPriceTick()

    #计算控制开关
    calcCommon = Parameter.common_forward
    calcDsl = Parameter.calcDsl_forward
    calcOwnl = Parameter.calcOwnl_forward
    calcFrsl = Parameter.calcFrsl_forward
    calcMultiSLT = Parameter.multiSTL_forward
    calcAtrsl = Parameter.calcAtrsl_forward
    calcGownl = Parameter.calcGownl_forward
    # 优化参数
    dsl_para_dic_list = []
    if calcDsl:
        dsl_target_list = Parameter.dsl_target_list_forward
        for dsl_target in dsl_target_list:
            dsl_para_dic_list.append({
                'para_name': str(dsl_target),
                'dsl_target': dsl_target
            })

    ownl_para_dic_list = []
    if calcOwnl:
        # stoplossList=[-0.022]
        ownl_protect_list = Parameter.ownl_protect_list_forward
        # winSwitchList=[0.009]
        ownl_floor_list = Parameter.ownl_floor_list_forward
        for ownl_protect in ownl_protect_list:
            for ownl_floor in ownl_floor_list:
                ownl_para_dic_list.append(
                    {
                        'para_name': "%.3f_%d" % (ownl_protect, ownl_floor),
                        'ownl_protect': ownl_protect,
                        'ownl_floor': ownl_floor * pricetick
                    }
                )

    frsl_para_dic_list = []
    if calcFrsl:
        frsl_target_list = Parameter.frsl_target_list_forward
        for frsl_target in frsl_target_list:
            frsl_para_dic_list.append({
                'para_name': str(frsl_target),
                'frsl_target': frsl_target
            })

    atrsl_para_dic_list = []
    if calcAtrsl:
        atr_pendant_n_list = Parameter.atr_pendant_n_list_forward
        atr_pendan_rate_list = Parameter.atr_pendant_rate_list_forward
        atr_yoyo_n_list = Parameter.atr_yoyo_n_list_forward
        atr_yoyo_rate_list = Parameter.atr_yoyo_rate_list_forward
        for atr_pendant_n in atr_pendant_n_list:
            for atr_pendant_rate in atr_pendan_rate_list:
                for atr_yoyo_n in atr_yoyo_n_list:
                    for atr_yoyo_rate in atr_yoyo_rate_list:
                        atrsl_para_dic_list.append(
                            {
                                'para_name': '%d_%.1f_%d_%.1f' % (
                                    atr_pendant_n, atr_pendant_rate, atr_yoyo_n, atr_yoyo_rate),
                                'atr_pendant_n': atr_pendant_n,
                                'atr_pendant_rate': atr_pendant_rate,
                                'atr_yoyo_n': atr_yoyo_n,
                                'atr_yoyo_rate': atr_yoyo_rate
                            }
                        )

    gownl_para_dic_list = []
    if calcGownl:
        gownl_protect_list = Parameter.gownl_protect_list_forward
        gownl_floor_list = Parameter.gownl_floor_list_forward
        gownl_step_list = Parameter.gownl_step_list_forward
        for gownl_protect in gownl_protect_list:
            for gownl_floor in gownl_floor_list:
                for gownl_step in gownl_step_list:
                    gownl_para_dic_list.append(
                        {
                            'para_name': '%.3f_%.1f_%.1f' % (gownl_protect, gownl_floor, gownl_step),
                            'gownl_protect': gownl_protect,
                            'gownl_floor': gownl_floor,
                            'gownl_step': gownl_step * pricetick
                        }
                    )

    #文件路径
    foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
    folderpath=resultpath+foldername+'\\'
    os.chdir(folderpath)
    if calcMultiSLT:
        sltlist = []
        if calcDsl:
            sltlist.append({'name': 'dsl',
                            'paralist': dsl_para_dic_list,
                            'folderPrefix': 'DynamicStopLoss',
                            'fileSuffix': 'resultDSL_by_tick.csv'})
        if calcOwnl:
            sltlist.append({'name': 'ownl',
                            'paralist': ownl_para_dic_list,
                            'folderPrefix': 'OnceWinNoLoss',
                            'fileSuffix': 'resultOWNL_by_tick.csv'})
        if calcFrsl:
            sltlist.append({'name': 'frsl',
                            'paralist': frsl_para_dic_list,
                            'folderPrefix': 'FixRateStopLoss',
                            'fileSuffix': 'resultFRSL_by_tick.csv'})
        if calcAtrsl:
            sltlist.append({'name': 'atrsl',
                            'paralist': atrsl_para_dic_list,
                            'folderPrefix': 'ATRSL',
                            'fileSuffix': 'resultATRSL_by_tick.csv'
                            })
        if calcGownl:
            sltlist.append({'name': 'gownl',
                            'paralist': gownl_para_dic_list,
                            'folderPrefix': 'GOWNL',
                            'fileSuffix': 'resultGOWNL_by_tick.csv'
                            })
        getmultiStlMonthParameter(strategyName,sltlist,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, newmonth)
    else:
        if calcCommon:
            colslist = mtf.getColumnsName(False)
            resultfilesuffix = ' result.csv'
            df = mtf.getMonthParameter(strategyName, startmonth, newmonth, symbolinfo, K_MIN, parasetlist, foldername,colslist, resultfilesuffix)
            filenamehead = ("%s_%s_%d_%s_parameter_common" % (strategyName, symbolinfo.domain_symbol, K_MIN, newmonth))
            df.to_csv(filenamehead + '.csv')

        if calcDsl:
            getDslMonthParameter(strategyName,dsl_para_dic_list,symbolinfo, K_MIN, parasetlist, folderpath, startmonth, newmonth)
        if calcOwnl:
            getownlMonthParameter(strategyName, ownl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startmonth, newmonth)
        if calcFrsl:
            getfrslMonthParameter(strategyName, frsl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startmonth, newmonth)
