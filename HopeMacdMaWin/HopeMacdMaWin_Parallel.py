# -*- coding: utf-8 -*-
'''
'''
import Hope_MACD_MA_Win
import pandas as pd
import numpy as np
import os
import DATA_CONSTANTS as DC
import multiprocessing
import HopeMacdMaWin_Parameter as Parameter
import ResultStatistics as RS

def getResult(strategyName,symbolinfo,K_MIN,setname,rawdata,para,positionRatio,initialCash,contractswaplist,indexcols):
    result = Hope_MACD_MA_Win.HopeWin_MACD_MA(symbolinfo,rawdata,para,positionRatio,initialCash,contractswaplist)
    result.to_csv(strategyName+' '+symbolinfo.symbol + str(K_MIN) + ' ' + setname + ' result.csv')
    results=RS.getStatisticsResult(result,False,indexcols)
    del result
    print results
    return [setname]+results #在这里附上setname

def getParallelResult(strategyParameter,resultpath,parasetlist,paranum,indexcols):

    strategyName = strategyParameter['strategyName']
    exchange_id = strategyParameter['exchange_id']
    sec_id = strategyParameter['sec_id']
    K_MIN = strategyParameter['K_MIN']
    startdate = strategyParameter['startdate']
    enddate = strategyParameter['enddate']
    symbol = '.'.join([exchange_id, sec_id])
    positionRatio = strategyParameter['positionRatio']
    initialCash = strategyParameter['initialCash']
    # ======================数据准备==============================================
    # 取合约信息
    symbolInfo = DC.SymbolInfo(symbol)

    # 取跨合约数据
    contractswaplist = DC.getContractSwaplist(symbol)
    swaplist = np.array(contractswaplist.swaputc)

    # 取K线数据
    rawdata = DC.getBarData(symbol, K_MIN, startdate + ' 00:00:00', enddate + ' 23:59:59').reset_index(drop=True)
    foldername = ' '.join([strategyName, exchange_id, sec_id, str(K_MIN)])
    try:
        os.chdir(resultpath)
        os.mkdir(foldername)
    except:
        print ("%s folder already exsist!" % foldername)
    os.chdir(foldername)

    # 多进程优化，启动一个对应CPU核心数量的进程池
    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    l = []
    resultlist = pd.DataFrame(columns=['Setname']+indexcols)
    for i in range(0, paranum):
        setname = parasetlist.ix[i, 'Setname']
        macd_s = parasetlist.ix[i, 'MACD_Short']
        macd_l = parasetlist.ix[i, 'MACD_Long']
        macd_m = parasetlist.ix[i, 'MACD_M']
        ma_n = parasetlist.ix[i,'MA_N']
        paraset = {
            'Setname':setname,
            'MACD_S': macd_s,
            'MACD_L': macd_l,
            'MACD_M': macd_m,
            'MA_N':ma_n
        }
        #l.append(getResult(symbolInfo, K_MIN, setname, rawdata, paraset, swaplist))
        l.append(pool.apply_async(getResult, (strategyName,symbolInfo, K_MIN, setname, rawdata, paraset, positionRatio,initialCash,swaplist,indexcols)))
    pool.close()
    pool.join()

    # 显示结果
    i = 0
    for res in l:
        resultlist.loc[i] = res.get()
        i += 1
    #print resultlist
    finalresults = ("%s %s %d finalresults.csv" % (strategyName,symbol, K_MIN))
    resultlist.to_csv(finalresults)
    return resultlist

if __name__=='__main__':
    #====================参数和文件夹设置======================================
    #文件路径
    upperpath=DC.getUpperPath(Parameter.folderLevel)
    resultpath = upperpath + Parameter.resultFolderName

    # 取参数集
    parasetlist = pd.read_csv(resultpath + Parameter.parasetname)
    paranum = parasetlist.shape[0]

    #indexcols
    indexcols=Parameter.ResultIndexDic
    #for d, f in Parameter.ResultIndexDic.items():
    #    if f:indexcols.append(d)

    #参数设置
    strategyParameterSet=[]
    if not Parameter.symbol_KMIN_opt_swtich:
        #单品种单周期模式
        paradic={
        'strategyName':Parameter.strategyName,
        'exchange_id': Parameter.exchange_id,
        'sec_id': Parameter.sec_id,
        'K_MIN': Parameter.K_MIN,
        'startdate': Parameter.startdate,
        'enddate' : Parameter.enddate,
        'positionRatio':Parameter.positionRatio,
        'initialCash': Parameter.initialCash
        }
        strategyParameterSet.append(paradic)
    else:
        #多品种多周期模式
        symbolset = pd.read_excel(resultpath + Parameter.symbol_KMIN_set_filename)
        symbolsetNum=symbolset.shape[0]
        for i in range(symbolsetNum):
            exchangeid = symbolset.ix[i, 'exchange_id']
            secid = symbolset.ix[i, 'sec_id']
            strategyParameterSet.append({
                'strategyName': symbolset.ix[i, 'strategyName'],
                'exchange_id': exchangeid,
                'sec_id': secid,
                'K_MIN': symbolset.ix[i,'K_MIN'],
                'startdate': symbolset.ix[i,'startdate'],
                'enddate': symbolset.ix[i,'enddate'],
                'positionRatio' : Parameter.positionRatio,
                'initialCash' : Parameter.initialCash
            }
            )
    allsymbolresult_cols=['Setname']+indexcols+[ 'strategyName','exchange_id','sec_id','K_MIN']
    allsymbolresult = pd.DataFrame(columns=allsymbolresult_cols)
    for strategyParameter in strategyParameterSet:
        r=getParallelResult(strategyParameter,resultpath,parasetlist,paranum,indexcols)
        r['strategyName']=strategyParameter['strategyName']
        r['exchange_id']=strategyParameter['exchange_id']
        r['sec_id'] = strategyParameter['sec_id']
        r['K_MIN'] = strategyParameter['K_MIN']
        allsymbolresult = pd.concat([allsymbolresult, r])
    allsymbolresult.reset_index(drop=False, inplace=True)
    os.chdir(resultpath)
    allsymbolresult.to_csv(Parameter.strategyName+"_symbol_KMIN_results.csv")