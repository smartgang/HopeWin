# -*- coding: utf-8 -*-

import DynamicStopLoss as dsl
import OnceWinNoLoss as ownl
import FixRateStopLoss as frsl
import DslOwnlClose as dslownl
import MultiStopLoss as msl
import StopLossLib.AtrStopLoss as atrsl
import StopLossLib.GOWNL as gownl
import DATA_CONSTANTS as DC
import pandas as pd
import os
import numpy as np
import multiprocessing
import datetime
import HopeMacdMaWin_Parameter as Parameter
import time


def getDSL(strategyName, symbolInfo, bar_type, dsl_para_dic_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress=False):
    symbol = symbolInfo.domain_symbol
    new_indexcols = []
    for i in indexcols:
        new_indexcols.append('new_' + i)
    allresultdf_cols = ['setname', 'slTarget', 'worknum'] + indexcols + new_indexcols
    allresultdf = pd.DataFrame(columns=allresultdf_cols)

    allnum = 0
    paranum = parasetlist.shape[0]
    for stoplossTarget_dic in dsl_para_dic_list:
        timestart = time.time()
        dslFolderName = ("DynamicStopLoss%s" % (stoplossTarget_dic['para_name']))
        try:
            os.mkdir(dslFolderName)  # 创建文件夹
        except:
            # print 'folder already exist'
            pass
        print ("stoplossTarget:%.3f" % stoplossTarget_dic['para_name'])

        resultdf = pd.DataFrame(columns=allresultdf_cols)
        setnum = 0
        numlist = range(0, paranum, 100)
        numlist.append(paranum)
        for n in range(1, len(numlist)):
            pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
            l = []
            for a in range(numlist[n - 1], numlist[n]):
                setname = parasetlist.ix[a, 'Setname']
                if not progress:
                    # l.append(dsl.dslCal(strategyName,symbolInfo, K_MIN, setname, oprdflist[a-a0], bar1mlist[a-a0], barxmlist[a-a0], positionRatio, initialCash, stoplossTarget, dslFolderName + '\\',
                    #                                       indexcols))
                    l.append(pool.apply_async(dsl.dslCal, (strategyName, symbolInfo, bar_type, setname, bar1mdic, barxmdic, result_para_dic, stoplossTarget_dic,
                                                           dslFolderName + '\\', indexcols)))
                else:
                    # l.append(dsl.progressDslCal(strategyName,symbolInfo, K_MIN, setname, bar1m, barxm, pricetick,
                    #                                               positionRatio, initialCash, stoplossTarget,
                    #                                               dslFolderName + '\\'))
                    l.append(pool.apply_async(dsl.progressDslCal, (strategyName,
                                                                   symbolInfo, bar_type, setname, bar1mdic, barxmdic, result_para_dic, stoplossTarget_dic,
                                                                   dslFolderName + '\\', indexcols)))
            pool.close()
            pool.join()

            for res in l:
                resultdf.loc[setnum] = res.get()
                allresultdf.loc[allnum] = resultdf.loc[setnum]
                setnum += 1
                allnum += 1
        resultdf.to_csv(dslFolderName + '\\' + strategyName + ' ' + symbol + str(bar_type) + ' finalresult_dsl%s.csv' % stoplossTarget_dic['para_name'], index=False)
        timeend = time.time()
        timecost = timeend - timestart
        print (u"dsl_%s 计算完毕，共%d组数据，总耗时%.3f秒,平均%.3f秒/组" % (stoplossTarget_dic['para_name'], paranum, timecost, timecost / paranum))
    allresultdf.to_csv(strategyName + ' ' + symbol + str(bar_type) + ' finalresult_dsl.csv', index=False)


def getOwnl(strategyName, symbolInfo, bar_type, ownl_para_dic_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress=False):
    symbol = symbolInfo.domain_symbol
    new_indexcols = []
    for i in indexcols:
        new_indexcols.append('new_' + i)
    allresultdf_cols = ['setname', 'winSwitch', 'worknum'] + indexcols + new_indexcols
    ownlallresultdf = pd.DataFrame(columns=allresultdf_cols)
    allnum = 0
    paranum = parasetlist.shape[0]
    for winSwitchDic in ownl_para_dic_list:
        timestart = time.time()
        ownlFolderName = "OnceWinNoLoss%s" % winSwitchDic['para_name']
        try:
            os.mkdir(ownlFolderName)  # 创建文件夹
        except:
            # print "dir already exist!"
            pass
        print ("OnceWinNoLoss WinSwitch:%s" % winSwitchDic['para_name'])

        ownlresultdf = pd.DataFrame(columns=allresultdf_cols)

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
                                              (strategyName, symbolInfo, bar_type, setname, bar1mdic, barxmdic, winSwitchDic, result_para_dic,
                                               ownlFolderName + '\\', indexcols)))
                else:
                    # l.append(ownl.progressOwnlCal(strategyName, symbolInfo, K_MIN, setname, bar1m, barxm, winSwitch,
                    #                           nolossThreshhold, positionRatio, initialCash,
                    #                           ownlFolderName + '\\'))
                    l.append(pool.apply_async(ownl.progressOwnlCal,
                                              (strategyName, symbolInfo, bar_type, setname, bar1mdic, barxmdic, winSwitchDic, result_para_dic,
                                               ownlFolderName + '\\', indexcols)))
            pool.close()
            pool.join()

            for res in l:
                ownlresultdf.loc[setnum] = res.get()
                ownlallresultdf.loc[allnum] = ownlresultdf.loc[setnum]
                setnum += 1
                allnum += 1
        # ownlresultdf['cashDelta'] = ownlresultdf['new_endcash'] - ownlresultdf['old_endcash']
        ownlresultdf.to_csv(ownlFolderName + '\\' + strategyName + ' ' + symbol + str(bar_type) + ' finalresult_ownl%s.csv' % winSwitchDic['para_name'], index=False)
        timeend = time.time()
        timecost = timeend - timestart
        print (u"ownl_%s 计算完毕，共%d组数据，总耗时%.3f秒,平均%.3f秒/组" % (winSwitchDic['para_name'], paranum, timecost, timecost / paranum))
    # ownlallresultdf['cashDelta'] = ownlallresultdf['new_endcash'] - ownlallresultdf['old_endcash']
    ownlallresultdf.to_csv(strategyName + ' ' + symbol + str(bar_type) + ' finalresult_ownl.csv', index=False)


def getFRSL(strategyName, symbolInfo, K_MIN, fixRateDicList, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress=False):
    symbol = symbolInfo.domain_symbol
    new_indexcols = []
    for i in indexcols:
        new_indexcols.append('new_' + i)
    allresultdf = pd.DataFrame(columns=['setname', 'fixRate', 'worknum'] + indexcols + new_indexcols)
    allnum = 0
    paranum = parasetlist.shape[0]
    for fixRateTarget in fixRateDicList:
        timestart = time.time()
        folderName = "FixRateStopLoss%s" % fixRateTarget['para_name']
        try:
            os.mkdir(folderName)  # 创建文件夹
        except:
            # print 'folder already exist'
            pass
        print ("fixRateTarget:%s" % fixRateTarget['para_name'])

        resultdf = pd.DataFrame(columns=['setname', 'fixRate', 'worknum'] + indexcols + new_indexcols)
        setnum = 0
        numlist = range(0, paranum, 100)
        numlist.append(paranum)
        for n in range(1, len(numlist)):
            pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
            l = []
            for a in range(numlist[n - 1], numlist[n]):
                setname = parasetlist.ix[a, 'Setname']
                if not progress:
                    # l.append(frsl.frslCal(strategyName,
                    #                                       symbolInfo, K_MIN, setname, bar1m, barxm, fixRateTarget, positionRatio,initialCash, folderName + '\\'))
                    l.append(pool.apply_async(frsl.frslCal, (strategyName,
                                                             symbolInfo, K_MIN, setname, bar1mdic, barxmdic, fixRateTarget, result_para_dic, folderName + '\\',
                                                             indexcols)))
                else:
                    l.append(pool.apply_async(frsl.progressFrslCal, (strategyName,
                                                                     symbolInfo, K_MIN, setname, bar1mdic, barxmdic, fixRateTarget, result_para_dic, folderName + '\\',
                                                                     indexcols)))
            pool.close()
            pool.join()

            for res in l:
                resultdf.loc[setnum] = res.get()
                allresultdf.loc[allnum] = resultdf.loc[setnum]
                setnum += 1
                allnum += 1
        # resultdf['cashDelta'] = resultdf['new_endcash'] - resultdf['old_endcash']
        resultdf.to_csv(folderName + '\\' + strategyName + ' ' + symbol + str(K_MIN) + ' finalresult_frsl%s.csv' % fixRateTarget['para_name'], index=False)
        timeend = time.time()
        timecost = timeend - timestart
        print (u"frsl_%s 计算完毕，共%d组数据，总耗时%.3f秒,平均%.3f秒/组" % (fixRateTarget['para_name'], paranum, timecost, timecost / paranum))
    # allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv(strategyName + ' ' + symbol + str(K_MIN) + ' finalresult_frsl.csv', index=False)


def get_atr_sl(strategyName, symbolInfo, bar_type, atr_para_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress=False):
    symbol = symbolInfo.domain_symbol
    new_indexcols = []
    for i in indexcols:
        new_indexcols.append('new_' + i)
    allresultdf = pd.DataFrame(columns=['setname', 'atr_sl_target', 'worknum'] + indexcols + new_indexcols)
    allnum = 0
    paranum = parasetlist.shape[0]
    for atr_para in atr_para_list:
        timestart = time.time()
        folderName = 'ATRSL%s' % atr_para['para_name']

        try:
            os.mkdir(folderName)  # 创建文件夹
        except:
            # print 'folder already exist'
            pass

        resultdf = pd.DataFrame(columns=['setname', 'atr_sl_target', 'worknum'] + indexcols + new_indexcols)
        setnum = 0
        numlist = range(0, paranum, 100)
        numlist.append(paranum)
        for n in range(1, len(numlist)):
            pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
            l = []
            for a in range(numlist[n - 1], numlist[n]):
                setname = parasetlist.ix[a, 'Setname']
                if not progress:
                    #l.append(atrsl.atrsl(strategyName, symbolInfo, bar_type, setname, bar1mdic, barxmdic, atr_para, result_para_dic, folderName + '\\',
                    #                     indexcols, timestart))
                    l.append(pool.apply_async(atrsl.atrsl, (strategyName,
                                                             symbolInfo, bar_type, setname, bar1mdic, barxmdic, atr_para, result_para_dic, folderName + '\\',
                                                             indexcols, timestart)))
                else:
                    l.append(pool.apply_async(atrsl.progress_atrsl, (strategyName,
                                                                     symbolInfo, K_MIN, setname, bar1mdic, barxmdic, atr_para, result_para_dic, folderName + '\\',
                                                                     indexcols)))
            pool.close()
            pool.join()

            for res in l:
                resultdf.loc[setnum] = res.get()
                allresultdf.loc[allnum] = resultdf.loc[setnum]
                setnum += 1
                allnum += 1
        # resultdf['cashDelta'] = resultdf['new_endcash'] - resultdf['old_endcash']
        resultdf.to_csv(folderName + '\\' + strategyName + ' ' + symbol + str(K_MIN) + ' finalresult_atrsl%s.csv' % folderName, index=False)
        timeend = time.time()
        timecost = timeend - timestart
        print (u"atr_%s 计算完毕，共%d组数据，总耗时%.3f秒,平均%.3f秒/组" % (folderName, paranum, timecost, timecost / paranum))
    # allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv(strategyName + ' ' + symbol + str(K_MIN) + ' finalresult_atrsl.csv', index=False)


def get_gownl(strategyName, symbolInfo, bar_type, gownl_para_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress=False):
    symbol = symbolInfo.domain_symbol
    new_indexcols = []
    for i in indexcols:
        new_indexcols.append('new_' + i)
    allresultdf = pd.DataFrame(columns=['setname', 'atr_sl_target', 'worknum'] + indexcols + new_indexcols)
    allnum = 0
    paranum = parasetlist.shape[0]
    for gownl_para in gownl_para_list:
        timestart = time.time()
        folderName = 'GOWNL%s' % gownl_para['para_name']

        try:
            os.mkdir(folderName)  # 创建文件夹
        except:
            # print 'folder already exist'
            pass

        resultdf = pd.DataFrame(columns=['setname', 'atr_sl_target', 'worknum'] + indexcols + new_indexcols)
        setnum = 0
        numlist = range(0, paranum, 100)
        numlist.append(paranum)
        for n in range(1, len(numlist)):
            pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
            l = []
            for a in range(numlist[n - 1], numlist[n]):
                setname = parasetlist.ix[a, 'Setname']
                if not progress:
                    #l.append(gownl.gownl(strategyName, symbolInfo, bar_type, setname, bar1mdic, barxmdic, gownl_para, result_para_dic, folderName + '\\',
                    #                     indexcols, timestart))
                    l.append(pool.apply_async(gownl.gownl, (strategyName,
                                                             symbolInfo, bar_type, setname, bar1mdic, barxmdic, gownl_para, result_para_dic, folderName + '\\',
                                                             indexcols, timestart)))
                else:
                    l.append(pool.apply_async(gownl.progress_gownl, (strategyName,
                                                                     symbolInfo, bar_type, setname, bar1mdic, barxmdic, gownl_para, result_para_dic, folderName + '\\',
                                                                     indexcols)))
            pool.close()
            pool.join()

            for res in l:
                resultdf.loc[setnum] = res.get()
                allresultdf.loc[allnum] = resultdf.loc[setnum]
                setnum += 1
                allnum += 1
        # resultdf['cashDelta'] = resultdf['new_endcash'] - resultdf['old_endcash']
        resultdf.to_csv(folderName + '\\' + strategyName + ' ' + symbol + str(bar_type) + ' finalresult_gownl%s.csv' % folderName, index=False)
        timeend = time.time()
        timecost = timeend - timestart
        print (u"gownl_%s 计算完毕，共%d组数据，总耗时%.3f秒,平均%.3f秒/组" % (folderName, paranum, timecost, timecost / paranum))
    # allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv(strategyName + ' ' + symbol + str(bar_type) + ' finalresult_gownl.csv', index=False)


def getDslOwnl(strategyName, symbolInfo, K_MIN, parasetlist, stoplossList, winSwitchList, result_para_dic, indexcols):
    symbol = symbolInfo.domain_symbol
    allresultdf = pd.DataFrame(
        columns=['setname', 'dslTarget', 'ownlWinSwtich', 'old_endcash', 'old_Annual', 'old_Sharpe', 'old_Drawback',
                 'old_SR', 'new_endcash', 'new_Annual', 'new_Sharpe', 'new_Drawback', 'new_SR',
                 'dslWorknum', 'ownlWorknum', 'dslRetDelta', 'ownlRetDelta'])
    allnum = 0
    paranum = parasetlist.shape[0]
    for stoplossTarget in stoplossList:
        for winSwitch in winSwitchList:
            dslFolderName = "DynamicStopLoss%.1f\\" % (stoplossTarget * 1000)
            ownlFolderName = "OnceWinNoLoss%.1f\\" % (winSwitch * 1000)
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
                                          (strategyName, symbolInfo, K_MIN, setname, stoplossTarget, winSwitch, result_para_dic, dslFolderName,
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
                allnum += 1
            resultfilename = ("%s %s%d finalresult_dsl%.3f_ownl%.3f.csv" % (strategyName, symbol, K_MIN, stoplossTarget, winSwitch))
            resultdf.to_csv(newfolder + '\\' + resultfilename)

    # allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv(strategyName + ' ' + symbol + str(K_MIN) + ' finalresult_dsl_ownl.csv')


def getMultiSLT(strategyName, symbolInfo, K_MIN, parasetlist, barxmdic, sltlist, result_para_dic, indexcols):
    """
    计算多个止损策略结合回测的结果
    :param strategyName:
    :param symbolInfo:
    :param K_MIN:
    :param parasetlist:
    :param sltlist:
    :param positionRatio:
    :param initialCash:
    :return:
    """
    symbol = symbolInfo.domain_symbol
    new_indexcols = []
    for i in indexcols:
        new_indexcols.append('new_' + i)
    allresultdf_cols = ['setname', 'slt', 'slWorkNum'] + indexcols + new_indexcols
    allresultdf = pd.DataFrame(columns=allresultdf_cols)

    allnum = 0
    paranum = parasetlist.shape[0]

    # dailyK = DC.generatDailyClose(barxm)

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
            #newfolder += (sltp['name'] + '_%.3f' % (sltp['sltValue']))
            v = sltp['sltValue']
            newfolder += "{}_{}".format(sltp['name'], v["para_name"])
        try:
            os.mkdir(newfolder)  # 创建文件夹
        except:
            pass
        print (newfolder)
        pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
        l = []
        for sn in range(0, paranum):
            setname = parasetlist.ix[sn, 'Setname']
            # l.append(msl.multiStopLosslCal(strategyName, symbolInfo, K_MIN, setname, sltset, positionRatio, initialCash,
            #                           newfolder + '\\'))
            l.append(pool.apply_async(msl.multiStopLosslCal,
                                      (strategyName, symbolInfo, K_MIN, setname, sltset, barxmdic, result_para_dic, newfolder, indexcols)))
        pool.close()
        pool.join()

        resultdf = pd.DataFrame(columns=allresultdf_cols)
        i = 0
        for res in l:
            resultdf.loc[i] = res.get()
            allresultdf.loc[allnum] = resultdf.loc[i]
            i += 1
            allnum += 1
        resultfilename = ("%s %s%d finalresult_multiSLT_%s.csv" % (strategyName, symbol, K_MIN, newfolder))
        resultdf.to_csv(newfolder + '\\' + resultfilename, index=False)

    allresultname = ''
    for slt in sltlist:
        allresultname += slt['name']
    # allresultdf['cashDelta'] = allresultdf['new_endcash'] - allresultdf['old_endcash']
    allresultdf.to_csv("%s %s%d finalresult_multiSLT_%s.csv" % (strategyName, symbol, K_MIN, allresultname), index=False)
    pass


if __name__ == '__main__':
    # 文件路径
    upperpath = DC.getUpperPath(Parameter.folderLevel)
    resultpath = upperpath + Parameter.resultFolderName

    # 取参数集
    #parasetlist = pd.read_csv(resultpath + Parameter.parasetname)
    #paranum = parasetlist.shape[0]

    # indexcols
    indexcols = Parameter.ResultIndexDic

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
            'result_para_dic': Parameter.result_para_dic,
            'progress': Parameter.progress_close,
            'calcDsl': Parameter.calcDsl_close,
            'calcOwnl': Parameter.calcOwnl_close,
            'calcFrsl': Parameter.calcFrsl_close,
            'calcMultiSLT': Parameter.calcMultiSLT_close,
            'dsl_target_list': Parameter.dsl_target_list_close,
            'ownl_protect_list': Parameter.ownl_protect_list_close,
            'ownl_floor_list': Parameter.ownl_floor_list_close,
            'frsl_target_list': Parameter.frsl_target_list_close,
            'calcAtrsl': Parameter.calcAtrsl_close,
            'atr_pendant_n_list': Parameter.atr_pendant_n_list_close,
            'atr_pendant_rate_list': Parameter.atr_pendant_rate_list_close,
            'atr_yoyo_n_list': Parameter.atr_yoyo_n_list_close,
            'atr_yoyo_rate_list': Parameter.atr_yoyo_rate_list_close,
            'calcGownl': Parameter.calcGownl_close,
            'gownl_protect_list': Parameter.gownl_protect_list_close,
            'gownl_floor_list': Parameter.gownl_floor_list_close,
            'gownl_step_list': Parameter.gownl_step_list_close
        }
        strategyParameterSet.append(paradic)
    else:
        # 多品种多周期模式
        symbolset = pd.read_excel(resultpath + Parameter.stoploss_set_filename, index_col='No')
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
                'para_Macd_S': symbolset.ix[i, 'MACD_Short'],
                'para_Macd_L': symbolset.ix[i, 'MACD_Long'],
                'para_Macd_M': symbolset.ix[i, 'MACD_M'],
                'para_MA_N': symbolset.ix[i, 'MA_N'],
                'progress': symbolset.ix[i, 'progress'],
                'calcDsl': symbolset.ix[i, 'calcDsl'],
                'calcOwnl': symbolset.ix[i, 'calcOwnl'],
                'calcFrsl': symbolset.ix[i, 'calcFrsl'],
                'calcMultiSLT': symbolset.ix[i, 'calcMultiSLT'],
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
        domain_symbol = '.'.join([exchange_id, sec_id])

        result_para_dic = strategyParameter['result_para_dic']

        symbolinfo = DC.SymbolInfo(domain_symbol, startdate, enddate)
        pricetick = symbolinfo.getPriceTick()

        # 计算控制开关
        progress = strategyParameter['progress']
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
                                    'para_name': '%d_%.1f_%d_%.1f' % (atr_pendant_n, atr_pendant_rate, atr_yoyo_n, atr_yoyo_rate),
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
        oprresultpath = resultpath + foldername + '\\'
        os.chdir(oprresultpath)

        # 原始数据处理
        # bar1m=DC.getBarData(symbol=symbol,K_MIN=60,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
        # barxm=DC.getBarData(symbol=symbol,K_MIN=K_MIN,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
        # bar1m计算longHigh,longLow,shortHigh,shortLow
        # bar1m=bar1mPrepare(bar1m)
        # bar1mdic = DC.getBarBySymbolList(domain_symbol, symbolinfo.getSymbolList(), 60, startdate, enddate)
        # barxmdic = DC.getBarBySymbolList(domain_symbol, symbolinfo.getSymbolList(), K_MIN, startdate, enddate)
        cols = ['open', 'high', 'low', 'close', 'strtime', 'utc_time', 'utc_endtime']
        # bar1mdic = DC.getBarDic(symbolinfo, 60, cols)
        # barxmdic = DC.getBarDic(symbolinfo, K_MIN, cols)
        bar1mdic = DC.getBarBySymbolList(domain_symbol, symbolinfo.getSymbolList(), 60, startdate, enddate, cols)
        barxmdic = DC.getBarBySymbolList(domain_symbol, symbolinfo.getSymbolList(), K_MIN, startdate, enddate, cols)

        try:
            # 取参数集
            parasetlist = pd.read_csv("%s %s %d %s" % (exchange_id, sec_id, K_MIN, Parameter.parasetname))
        except:
            if not Parameter.symbol_KMIN_opt_swtich:
                para_list_dic = None    # 单品种模式使用默认参数
            else:
                # 如果没有，则直接生成
                para_list_dic = {
                    'MACD_S':strategyParameter['para_Macd_S'],
                    'MACD_L':strategyParameter['para_Macd_L'],
                    'MACD_M':strategyParameter['para_Macd_M'],
                    'MA_N': strategyParameter['para_MA_N']
                }
            parasetlist = Parameter.generat_para_file(para_list_dic)
            parasetlist.to_csv("%s %s %d %s" % (exchange_id, sec_id, K_MIN, Parameter.parasetname))
        paranum = parasetlist.shape[0]

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
            getMultiSLT(strategyName, symbolinfo, K_MIN, parasetlist, barxmdic, sltlist, result_para_dic, indexcols)
        else:
            if calcDsl:
                getDSL(strategyName, symbolinfo, K_MIN, dsl_para_dic_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress)

            if calcOwnl:
                getOwnl(strategyName, symbolinfo, K_MIN, ownl_para_dic_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress)

            if calcFrsl:
                getFRSL(strategyName, symbolinfo, K_MIN, frsl_para_dic_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress)

            if calcAtrsl:
                get_atr_sl(strategyName, symbolinfo, K_MIN, atrsl_para_dic_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress=False)

            if calcGownl:
                get_gownl(strategyName, symbolinfo, K_MIN, gownl_para_dic_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols)
