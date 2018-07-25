# -*- coding: utf-8 -*-

from FixValueStopLoss import fix_value_stop_loss
import DATA_CONSTANTS as DC
import pandas as pd
import os
import numpy as np
import multiprocessing
import datetime
import IntradayFryWin_Parameter as Parameter
import time
import ATR

def getFVSL(strategyName, symbolInfo, K_MIN, spr_slr_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress=False):
    symbol = symbolInfo.domain_symbol
    new_indexcols = []
    for i in indexcols:
        new_indexcols.append('new_'+i)
    allresultdf_cols=['setname', 'spr', 'slr', 'worknum']+indexcols+new_indexcols
    allresultdf = pd.DataFrame(columns=allresultdf_cols)

    # 加入ATR取值
    for v in barxmdic.values():
        v['TR'], v['ATR'] = ATR.ATR(v.high, v.low, v.close)

    allnum = 0
    paranum=parasetlist.shape[0]
    for spr_slr in spr_slr_list:
        spr = spr_slr[0]
        slr = spr_slr[1]
        timestart = time.time()
        folderName = ("spr%.1f_slr%.1f" % (spr, slr))
        try:
            os.mkdir(folderName)  # 创建文件夹
        except:
            #print 'folder already exist'
            pass
        print (folderName)

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
                    l.append(fix_value_stop_loss(strategyName, symbolInfo, K_MIN, setname, bar1mdic, barxmdic, result_para_dic, spr, slr,
                                                                                folderName + '\\', indexcols))
                    #l.append(pool.apply_async(fix_value_stop_loss, (strategyName, symbolInfo, K_MIN, setname, bar1mdic, barxmdic, result_para_dic, spr, slr,
                    #                                       folderName + '\\', indexcols)))
                else:
                    pass
                    #l.append(dsl.progressDslCal(strategyName,symbolInfo, K_MIN, setname, bar1m, barxm, pricetick,
                    #                                               positionRatio, initialCash, stoplossTarget,
                    #                                               dslFolderName + '\\'))
            pool.close()
            pool.join()

            for res in l:
                resultdf.loc[setnum] = res.get()
                allresultdf.loc[allnum] = resultdf.loc[setnum]
                setnum += 1
                allnum += 1
        resultdf.to_csv(folderName + '\\' + strategyName + ' ' + symbol + str(K_MIN) + ' finalresult_%s.csv' % folderName, index=False)
        timeend = time.time()
        timecost = timeend - timestart
        print (u"%s 计算完毕，共%d组数据，总耗时%.3f秒,平均%.3f秒/组" % (folderName,paranum, timecost, timecost / paranum))
    allresultdf.to_csv(strategyName + ' ' + symbol + str(K_MIN) + ' finalresult_dsl.csv', index=False)


if __name__=='__main__':
    # 文件路径
    upperpath = DC.getUpperPath(Parameter.folderLevel)
    resultpath = upperpath + Parameter.resultFolderName

    # 取参数集
    parasetlist = pd.read_csv(resultpath + Parameter.parasetname)
    paranum = parasetlist.shape[0]

    #indexcols
    indexcols=Parameter.ResultIndexDic

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
            'progress':Parameter.progress_close,
            'spr_slr_list': Parameter.spr_slr_list
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
                'result_para_dic': Parameter.result_para_dic,
                'progress':symbolset.ix[i,'progress'],
                'spr_slr_list':[[symbolset.ix[i, 'spr'], symbolset.ix[i, 'slr']]]
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

        #计算控制开关
        progress = strategyParameter['progress']
        spr_slr_list = strategyParameter['spr_slr_list']
        #文件路径
        foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
        oprresultpath=resultpath+foldername+'\\'
        os.chdir(oprresultpath)

        # 原始数据处理
        # bar1m=DC.getBarData(symbol=symbol,K_MIN=60,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
        # barxm=DC.getBarData(symbol=symbol,K_MIN=K_MIN,starttime=startdate+' 00:00:00',endtime=enddate+' 23:59:59')
        # bar1m计算longHigh,longLow,shortHigh,shortLow
        # bar1m=bar1mPrepare(bar1m)
        # bar1mdic = DC.getBarBySymbolList(domain_symbol, symbolinfo.getSymbolList(), 60, startdate, enddate)
        # barxmdic = DC.getBarBySymbolList(domain_symbol, symbolinfo.getSymbolList(), K_MIN, startdate, enddate)
        cols = ['open', 'high', 'low', 'close', 'strtime', 'utc_time', 'utc_endtime']
        bar1mdic = DC.getBarDic(symbolinfo, 60, cols)
        barxmdic = DC.getBarDic(symbolinfo, K_MIN, cols)
        getFVSL(strategyName, symbolinfo, K_MIN, spr_slr_list, parasetlist, bar1mdic, barxmdic, result_para_dic, indexcols, progress)
