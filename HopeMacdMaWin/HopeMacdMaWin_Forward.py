# -*- coding: utf-8 -*-
import multiTargetForward as mtf
from datetime import datetime
import pandas as pd
import DATA_CONSTANTS as DC
import os
import multiprocessing
import HopeMacdMaWin_Parameter as Parameter
import numpy as np


def getForward(strategyName, symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, result_para_dic, indexcolsFlag,
               resultfilesuffix):
    symbol = symbolinfo.domain_symbol
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

    initialCash = result_para_dic['initialCash']
    positionRatio = result_para_dic['positionRatio']

    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    l = []
    for whiteWindows in windowsSet:
        # l.append(mtf.runPara(strategyName,whiteWindows, symbolinfo, K_MIN, parasetlist, monthlist, rawdatapath, forwordresultpath, forwardrankpath, colslist, resultfilesuffix))
        l.append(pool.apply_async(mtf.runPara, (
        strategyName, whiteWindows, symbolinfo, K_MIN, parasetlist, monthlist, rawdatapath, forwordresultpath, forwardrankpath, colslist, resultfilesuffix)))
    pool.close()
    pool.join()
    mtf.calGrayResult(strategyName, symbol, K_MIN, windowsSet, forwardrankpath, rawdatapath)
    indexcols = Parameter.ResultIndexDic

    # rawdata = DC.getBarData(symbol, K_MIN, monthlist[12] + '-01 00:00:00', enddate + ' 23:59:59').reset_index(drop=True)
    cols = ['open', 'high', 'low', 'close', 'strtime', 'utc_time', 'utc_endtime']
    barxmdic = DC.getBarDic(symbolinfo, K_MIN, cols)

    mtf.calOprResult(strategyName, rawdatapath, symbolinfo, K_MIN, nextmonth, colslist, barxmdic, positionRatio, initialCash, indexcols, indexcolsFlag, resultfilesuffix)
    endtime = datetime.now()
    print starttime
    print endtime


def getDslForward(strategyName, dsl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic):
    print ('DSL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultDSL_by_tick.csv'
    indexcolsFlag = True
    for dsl_para in dsl_para_dic_list:
        rawdatapath = folderpath + "DynamicStopLoss%s\\" % dsl_para['para_name']
        getForward(strategyName, symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, result_para_dic, indexcolsFlag,
                   resultfilesuffix)
    print ('DSL forward finished!')


def getownlForward(strategyName, ownl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic):
    print ('OWNL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultOWNL_by_tick.csv'
    indexcolsFlag = True
    for ownl_para_dic in ownl_para_dic_list:
        rawdatapath = folderpath + "OnceWinNoLoss%s\\" % ownl_para_dic['para_name']
        getForward(strategyName, symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, result_para_dic, indexcolsFlag,
                   resultfilesuffix)
    print ('OWNL forward finished!')


def frsl_forward(strategyName, para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic):
    print ('FRSL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultFRSL_by_tick.csv'
    indexcolsFlag = True
    for para_dic in para_dic_list:
        rawdatapath = folderpath + "FixRateStopLoss%s\\" % para_dic['para_name']
        getForward(strategyName, symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, result_para_dic, indexcolsFlag,
                   resultfilesuffix)
    print ('FRSL forward finished!')


def atrsl_forward(strategyName, atrsl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic):
    print ('ATRSL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultATRSL_by_tick.csv'
    indexcolsFlag = True
    for atrsl_para_dic in atrsl_para_dic_list:
        rawdatapath = folderpath + "ATRSL%s\\" % atrsl_para_dic['para_name']
        getForward(strategyName, symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, result_para_dic, indexcolsFlag,
                   resultfilesuffix)
    print ('ATRSL forward finished!')


def gownl_forward(strategyName, para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic):
    print ('GOWNL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'resultGOWNL_by_tick.csv'
    indexcolsFlag = True
    for para_dic in para_dic_list:
        rawdatapath = folderpath + "GOWNL%s\\" % para_dic['para_name']
        getForward(strategyName, symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, result_para_dic, indexcolsFlag,
                   resultfilesuffix)
    print ('GOWNL forward finished!')


def getdsl_ownlForward(strategyName, dsl_ownl_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic):
    print ('DSL_OWNL forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'result_dsl_ownl.csv'
    indexcolsFlag = True
    for dsl_ownl in dsl_ownl_list:
        newfolder = ("dsl_%.3f_ownl_%.3f\\" % (dsl_ownl[0], dsl_ownl[1]))
        rawdatapath = folderpath + newfolder  # ！！正常:'\\'，双止损：填上'\\+双止损目标文件夹\\'
        getForward(strategyName, symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, result_para_dic, indexcolsFlag,
                   resultfilesuffix)
    print ('DSL_OWNL forward finished!')


def getMultiSltForward(strategyName, sltlist, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic):
    '''
    混合止损推进
    '''
    print ('multiSLT forward start!')
    colslist = mtf.getColumnsName(True)
    resultfilesuffix = 'result_multiSLT.csv'
    indexcolsFlag = True
    # 先生成参数列表
    allSltSetList = []  # 这是一个二维的参数列表，每一个元素是一个止损目标的参数dic列表
    for slt in sltlist:
        sltset = []
        for t in slt['paralist']:
            sltset.append({'name': slt['name'],
                           'sltValue': t,   # t是一个参数字典
                           'folder': ("%s%s\\" % (slt['folderPrefix'], t['para_name'])),
                           'fileSuffix': slt['fileSuffix']
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
    print finalSltSetList
    for sltset in finalSltSetList:
        newfolder = ''
        for sltp in sltset:
            v = sltp['sltValue']
            newfolder += "{}_{} ".format(sltp['name'], v["para_name"])
        rawdatapath = folderpath + newfolder + '\\'
        print ("multiSTL Target:%s" % newfolder)
        getForward(strategyName, symbolinfo, K_MIN, parasetlist, rawdatapath, startdate, enddate, nextmonth, windowsSet, colslist, result_para_dic, indexcolsFlag, resultfilesuffix)
    print ('multiSTL forward finished!')


if __name__ == '__main__':
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
            'result_para_dic': Parameter.result_para_dic,
            # 'nextmonth':Parameter.nextMonthName,
            'commonForward': Parameter.common_forward,
            'calcDsl': Parameter.calcDsl_forward,
            'calcOwnl': Parameter.calcOwnl_forward,
            'calcFrsl': Parameter.calcFrsl_forward,
            'dsl_target_list': Parameter.dsl_target_list_forward,
            'ownl_protect_list': Parameter.ownl_protect_list_forward,
            'ownl_floor_list': Parameter.ownl_floor_list_forward,
            'frsl_target_list': Parameter.frsl_target_list_forward,
            'calcAtrsl': Parameter.calcAtrsl_forward,
            'atr_pendant_n_list': Parameter.atr_pendant_n_list_forward,
            'atr_pendant_rate_list': Parameter.atr_pendant_rate_list_forward,
            'atr_yoyo_n_list': Parameter.atr_yoyo_n_list_forward,
            'atr_yoyo_rate_list': Parameter.atr_yoyo_rate_list_forward,
            'calcGownl': Parameter.calcGownl_forward,
            'gownl_protect_list': Parameter.gownl_protect_list_forward,
            'gownl_floor_list': Parameter.gownl_floor_list_forward,
            'gownl_step_list': Parameter.gownl_step_list_forward,
            'multiSTL': Parameter.multiSTL_forward  # 混合止损推进开关
        }
        strategyParameterSet.append(paradic)
    else:
        # 多品种多周期模式
        symbolset = pd.read_excel(resultpath + Parameter.forward_set_filename, index_col='No')
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
                'result_para_dic': Parameter.result_para_dic,
                # 'nextmonth':symbolset.ix[i,'nextmonth'],
                'commonForward': symbolset.ix[i, 'commonForward'],
                'calcDsl': symbolset.ix[i, 'calcDsl'],
                'calcOwnl': symbolset.ix[i, 'calcOwnl'],
                'calcFrsl': symbolset.ix[i, 'calcFrsl'],
                'calcDslOwnl': symbolset.ix[i, 'calcDslOwnl'],
                'multiSTL': symbolset.ix[i, 'multiSTL'],  # 混合止损推进开关
                'dsl_target_list': Parameter.para_str_to_float(symbolset.ix[i, 'dsl_target']),
                'ownl_protect_list': Parameter.para_str_to_float(symbolset.ix[i, 'ownl_protect']),
                'ownl_floor_list': Parameter.para_str_to_float(symbolset.ix[i, 'ownl_floor']),
                'frsl_target_list': Parameter.para_str_to_float(symbolset.ix[i, 'frsl_target']),
                'calcAtrsl': symbolset.ix[i, 'calcAtrsl'],
                'atr_pendant_n_list': Parameter.para_str_to_float(symbolset.ix[i, 'atr_pendant_n']),
                'atr_pendant_rate_list': Parameter.para_str_to_float(symbolset.ix[i, 'atr_pendant_rate']),
                'atr_yoyo_n_list': Parameter.para_str_to_float(symbolset.ix[i, 'atr_yoyo_n']),
                'atr_yoyo_rate_list': Parameter.para_str_to_float(symbolset.ix[i, 'atr_yoyo_n']),
                'calcGownl': symbolset.ix[i, 'calcGownl'],
                'gownl_protect_list': Parameter.para_str_to_float(symbolset.ix[i, 'gownl_protect']),
                'gownl_floor_list': Parameter.para_str_to_float(symbolset.ix[i, 'gownl_floor']),
                'gownl_step_list': Parameter.para_str_to_float(symbolset.ix[i, 'gownl_step'])
            }
            )

    for strategyParameter in strategyParameterSet:

        strategyName = strategyParameter['strategyName']
        exchange_id = strategyParameter['exchange_id']
        sec_id = strategyParameter['sec_id']
        K_MIN = strategyParameter['K_MIN']
        startdate = strategyParameter['startdate']
        enddate = strategyParameter['enddate']
        # nextmonth = strategyParameter['nextmonth']
        nextmonth = enddate[:7]
        symbol = '.'.join([exchange_id, sec_id])

        result_para_dic = strategyParameter['result_para_dic']

        symbolinfo = DC.SymbolInfo(symbol, startdate, enddate)
        slip = DC.getSlip(symbol)
        pricetick = DC.getPriceTick(symbol)

        # 计算控制开关
        progress = strategyParameter['progress']
        calcCommon = strategyParameter['commonForward']
        calcDsl = strategyParameter['calcDsl']
        calcOwnl = strategyParameter['calcOwnl']
        calcFrsl = strategyParameter['calcFrsl']
        calcMultiSLT = strategyParameter['calcMultiSLT']
        calcAtrsl = strategyParameter['calcAtrsl']
        calcGownl = strategyParameter['calcGownl']
        # 优化参数
        dsl_para_dic_list = []
        if calcDsl:
            dsl_target_list = strategyParameter['dsl_target_list']
            for dsl_target in dsl_target_list:
                dsl_para_dic_list.append({
                    'para_name': str(dsl_target),
                    'dsl_target': dsl_target
                })

        ownl_para_dic_list = []
        if calcOwnl:
            # stoplossList=[-0.022]
            ownl_protect_list = strategyParameter['ownl_protect_list']
            # winSwitchList=[0.009]
            ownl_floor_list = strategyParameter['ownl_floor_list']
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
            frsl_target_list = strategyParameter['frsl_target_list']
            for frsl_target in frsl_target_list:
                frsl_para_dic_list.append({
                    'para_name': str(frsl_target),
                    'frsl_target': frsl_target
                })

        atrsl_para_dic_list = []
        if calcAtrsl:
            atr_pendant_n_list = strategyParameter['atr_pendant_n_list']
            atr_pendan_rate_list = strategyParameter['atr_pendant_rate_list']
            atr_yoyo_n_list = strategyParameter['atr_yoyo_n_list']
            atr_yoyo_rate_list = strategyParameter['atr_yoyo_rate_list']
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
            gownl_protect_list = strategyParameter['gownl_protect_list']
            gownl_floor_list = strategyParameter['gownl_floor_list']
            gownl_step_list = strategyParameter['gownl_step_list']
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

        # 文件路径
        foldername = ' '.join([strategyName, exchange_id, sec_id, str(K_MIN)])
        folderpath = resultpath + foldername + '\\'
        os.chdir(folderpath)

        parasetlist = pd.read_csv(resultpath + Parameter.parasetname)

        if calcMultiSLT:
            # 混合止损推进打开的情况下，只做混合止损推进
            sltlist = []
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
            getMultiSltForward(strategyName, sltlist, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic)
            pass
        else:
            if calcCommon:
                colslist = mtf.getColumnsName(False)
                resultfilesuffix = 'result.csv'
                indexcolsFlag = False
                getForward(strategyName, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, colslist, result_para_dic, indexcolsFlag,
                           resultfilesuffix)
            if calcDsl:
                getDslForward(strategyName, dsl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic)
            if calcOwnl:
                getownlForward(strategyName, ownl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate, enddate, nextmonth, windowsSet, result_para_dic)
            if calcFrsl:
                frsl_forward(strategyName, frsl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate,
                               enddate, nextmonth, windowsSet, result_para_dic)
            if calcAtrsl:
                atrsl_forward(strategyName, atrsl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate,
                               enddate, nextmonth, windowsSet, result_para_dic)
            if calcGownl:
                gownl_forward(strategyName, gownl_para_dic_list, symbolinfo, K_MIN, parasetlist, folderpath, startdate,
                               enddate, nextmonth, windowsSet, result_para_dic)
