# -*- coding: utf-8 -*-
'''
SAR：
    15min数据计算SAR
    SAR计算过程中，标出反转点
MACD：
    1小时数据计算MACD
    将MACD合并到SAR数据中
    计算MACD的金叉和死叉
开多：
    SAR金叉
    MACD为正
平多：
    SAR死叉，或者MACD死叉
开空：
    SAR死叉
    MACD为负
平空：
    SAR金叉，或MACD金叉
流程 ：
    先提取出SAR多空持仓
    再用MACD进行开仓过滤
    再用MACD进行平仓过滤
    再用双止损修正
'''
import MA
import SAR
from Boll import BOLL
from ATR import ATR
import pandas as pd
import os
import DATA_CONSTANTS as DC
import numpy as np
import multiprocessing
import ResultStatistics as RS
from prepareMACD import *

def removeContractSwap(resultlist,contractswaplist):
    results=resultlist
    resultnum=results.shape[0]
    i=0
    for utc in contractswaplist:
        while i<resultnum:
            result=results.loc[i]
            if result.openutc< utc and result.closeutc>utc:
                results.drop(i, inplace=True)
                i+=1
                break
            if result.openutc> utc and result.closeutc>utc:
                i+=1
                break
            i+=1
    results = results.reset_index(drop=True)
    return results

def HopeWin(symbolInfo,setname,K_MIN_SAR,K_MIN_MACD,startdate,enddate,macdParaSet,contractswaplist,calcResult=True):
    print setname
    rawdata_sar = pd.read_csv('rawdata_sar.csv')
    rawdata_macd = DC.getBarData(symbolInfo.symbol, K_MIN_MACD, startdate + " 00:00:00", enddate + " 23:59:59")
    MACD_S=macdParaSet['MACD_S']
    MACD_L = macdParaSet['MACD_L']
    MACD_M = macdParaSet['MACD_M']

    '''
    # 计算MACD
    macd = MA.calMACD(rawdata_macd['close'], MACD_S, MACD_L, MACD_M)
    rawdata_macd['DIF'] = macd[0]
    rawdata_macd['DEA'] = macd[1]

    # 将MACD合并到SAR数据
    rawdata_sar.set_index('utc_time', inplace=True)
    rawdata_macd.set_index('utc_endtime', inplace=True) #使用macd的结束时间跟SAR的开始时间对齐，相当于使用上1小时的MACD值
    rawdata_sar['DIF'] = rawdata_macd['DIF']
    rawdata_sar['DEA'] = rawdata_macd['DEA']
    rawdata_sar.fillna(method='ffill', inplace=True)
    rawdata_sar.reset_index(inplace=True)
    '''
    rawdata_sar=prepareMACD(rawdata_sar,rawdata_macd,MACD_S,MACD_L,MACD_M)
    # 计算MACD的金叉和死叉
    rawdata_sar['MACD_True'], rawdata_sar['MACD_Cross'] = MA.dfCross(rawdata_sar, 'DIF', 'DEA')

    # ================================ 找出买卖点================================================
    # 1.先找出SAR金叉的买卖点
    # 2.找到结合判决条件的买点
    # 3.从MA买点中滤出真实买卖点
    # 取出金叉点
    goldcrosslist = pd.DataFrame({'goldcrosstime': rawdata_sar.loc[rawdata_sar['SAR_R'] == 1, 'strtime']})
    goldcrosslist['goldcrossutc'] = rawdata_sar.loc[rawdata_sar['SAR_R'] == 1, 'utc_time']
    goldcrosslist['goldcrossindex'] = rawdata_sar.loc[rawdata_sar['SAR_R'] == 1, 'Unnamed: 0']
    goldcrosslist['goldcrossprice'] = rawdata_sar.loc[rawdata_sar['SAR_R'] == 1, 'close']

    # 取出死叉点
    deathcrosslist = pd.DataFrame({'deathcrosstime': rawdata_sar.loc[rawdata_sar['SAR_R'] == -1, 'strtime']})
    deathcrosslist['deathcrossutc'] = rawdata_sar.loc[rawdata_sar['SAR_R'] == -1, 'utc_time']
    deathcrosslist['deathcrossindex'] = rawdata_sar.loc[rawdata_sar['SAR_R'] == -1, 'Unnamed: 0']
    deathcrosslist['deathcrossprice'] = rawdata_sar.loc[rawdata_sar['SAR_R'] == -1, 'close']
    goldcrosslist = goldcrosslist.reset_index(drop=True)
    deathcrosslist = deathcrosslist.reset_index(drop=True)

    # 生成多仓序列（金叉在前，死叉在后）
    if goldcrosslist.ix[0, 'goldcrossindex'] < deathcrosslist.ix[0, 'deathcrossindex']:
        longcrosslist = pd.concat([goldcrosslist, deathcrosslist], axis=1)
    else:  # 如果第一个死叉的序号在金叉前，则要将死叉往上移1格
        longcrosslist = pd.concat([goldcrosslist, deathcrosslist.shift(-1).fillna(0)], axis=1)
    longcrosslist = longcrosslist.set_index(pd.Index(longcrosslist['goldcrossindex']), drop=True)

    # 生成空仓序列（死叉在前，金叉在后）
    if deathcrosslist.ix[0, 'deathcrossindex'] < goldcrosslist.ix[0, 'goldcrossindex']:
        shortcrosslist = pd.concat([deathcrosslist, goldcrosslist], axis=1)
    else:  # 如果第一个金叉的序号在死叉前，则要将金叉往上移1格
        shortcrosslist = pd.concat([deathcrosslist, goldcrosslist.shift(-1).fillna(0)], axis=1)
    shortcrosslist = shortcrosslist.set_index(pd.Index(shortcrosslist['deathcrossindex']), drop=True)

    # 取出开多序号和开空序号
    #SAR金叉做多，死叉做空
    #做多过滤：MACD处于金叉期间且DIF>=0
    #做空过滤：MACD处于死叉期间且DIF<0
    openlongindex = rawdata_sar.loc[
        (rawdata_sar['SAR_R'] == 1) & (rawdata_sar['MACD_True'] == 1) & (rawdata_sar['DIF'] >= 0) & (
        rawdata_sar['boll_filter'] == 1)].index
    openshortindex = rawdata_sar.loc[
        (rawdata_sar['SAR_R'] == -1) & (rawdata_sar['MACD_True'] == -1) & (rawdata_sar['DIF'] < 0) & (
        rawdata_sar['boll_filter'] == 1)].index

    # 从多仓序列中取出开多序号的内容，即为开多操作
    longopr = longcrosslist.loc[openlongindex]
    longopr['tradetype'] = 1
    longopr.rename(columns={'goldcrosstime': 'opentime',
                            'goldcrossutc': 'openutc',
                            'goldcrossindex': 'openindex',
                            'goldcrossprice': 'openprice',
                            'deathcrosstime': 'closetime',
                            'deathcrossutc': 'closeutc',
                            'deathcrossindex': 'closeindex',
                            'deathcrossprice': 'closeprice'}, inplace=True)

    # 从空仓序列中取出开空序号的内容，即为开空操作
    shortopr = shortcrosslist.loc[openshortindex]
    shortopr['tradetype'] = -1
    shortopr.rename(columns={'deathcrosstime': 'opentime',
                             'deathcrossutc': 'openutc',
                             'deathcrossindex': 'openindex',
                             'deathcrossprice': 'openprice',
                             'goldcrosstime': 'closetime',
                             'goldcrossutc': 'closeutc',
                             'goldcrossindex': 'closeindex',
                             'goldcrossprice': 'closeprice'}, inplace=True)

    # 结果分析
    result = pd.concat([longopr, shortopr])
    result = result.sort_index()
    result = result.reset_index(drop=True)
    result.drop(result.shape[0] - 1, inplace=True)
    # 去掉跨合约的操作
    result = removeContractSwap(result, contractswaplist)
    initial_cash = 20000
    margin_rate = 0.2
    slip = symbolInfo.getSlip()
    multiplier = symbolInfo.getMultiplier()
    poundgeType, poundgeFee, poundgeRate = symbolInfo.getPoundage()

    result['ret'] = ((result['closeprice'] - result['openprice']) * result['tradetype']) - slip
    result['ret_r'] = result['ret'] / result['openprice']
    results={}
    if calcResult:
        firsttradecash = initial_cash / margin_rate
        result['commission_fee'] = 0
        if poundgeType == symbolInfo.POUNDGE_TYPE_RATE:
            result.ix[0, 'commission_fee'] = firsttradecash * poundgeRate * 2
        else:
            result.ix[0, 'commission_fee'] = firsttradecash / (multiplier * result.ix[0, 'openprice']) * poundgeFee * 2
        result['per earn'] = 0  # 单笔盈亏
        result['own cash'] = 0  # 自有资金线
        result['trade money'] = 0  # 杠杆后的可交易资金线

        result.ix[0, 'per earn'] = firsttradecash * result.ix[0, 'ret_r']
        result.ix[0, 'own cash'] = initial_cash + result.ix[0, 'per earn'] - result.ix[0, 'commission_fee']
        result.ix[0, 'trade money'] = result.ix[0, 'own cash'] / margin_rate
        oprtimes = result.shape[0]
        for i in np.arange(1, oprtimes):
            # 根据手续费类型计算手续费
            if poundgeType == symbolInfo.POUNDGE_TYPE_RATE:
                commission = result.ix[i - 1, 'trade money'] * poundgeRate * 2
            else:
                commission = result.ix[i - 1, 'trade money'] / (multiplier * result.ix[i, 'openprice']) * poundgeFee * 2
            perearn = result.ix[i - 1, 'trade money'] * result.ix[i, 'ret_r']
            owncash = result.ix[i - 1, 'own cash'] + perearn - commission
            result.ix[i, 'own cash'] = owncash
            result.ix[i, 'commission_fee'] = commission
            result.ix[i, 'per earn'] = perearn
            result.ix[i, 'trade money'] = owncash / margin_rate

        endcash = result.ix[oprtimes - 1, 'own cash']
        Annual = RS.annual_return(result)
        Sharpe = RS.sharpe_ratio(result)
        DrawBack = RS.max_drawback(result)[0]
        SR = RS.success_rate(result)
        max_single_loss_rate = abs(result['ret_r'].min())

        results = {
            'Setname':setname,
            'MACD_S':MACD_S,
            'MACD_L':MACD_L,
            'MACD_M':MACD_M,
            'opentimes': oprtimes,
            'end_cash': endcash,
            'SR': SR,
            'Annual':Annual,
            'Sharpe':Sharpe,
            'DrawBack':DrawBack,
            'max_single_loss_rate': max_single_loss_rate
        }
        print results
    filename = ("%s%d %d %s result.csv" % (symbolInfo.symbol, K_MIN_SAR, K_MIN_MACD,setname))
    result.to_csv(filename)
    del rawdata_sar
    del rawdata_macd
    return results

if __name__=='__main__':
    #====================参数和文件夹设置======================================
    #参数设置
    exchange_id = 'DCE'
    sec_id='I'
    K_MIN_SAR = 900
    K_MIN_MACD = 3600
    symbol = '.'.join([exchange_id, sec_id])
    startdate='2016-01-01'
    enddate = '2017-12-31'

    #SAR参数
    AF_Step = 0.02
    AF_MAX = 0.2
    #MACD参数
    MACD_SHORT = 6
    MACD_LONG = 30
    MACD_M = 9
    # BOLL参数
    BOLL_N = 26
    BOLL_M = 26
    BOLL_P = 2
    # ATR参数
    ATR_N = 26

    #文件路径
    upperpath=DC.getUpperPath(1)
    foldername = ' '.join([exchange_id, sec_id, str(K_MIN_SAR),str(K_MIN_MACD)])
    resultpath=upperpath+"\\Results\\"
    os.chdir(resultpath)
    try:
        os.mkdir(foldername)
    except:
        print ("%s folder already exsist!" %foldername)
    os.chdir(foldername)

    # ======================数据准备==============================================
    #取参数集
    parasetlist=pd.read_csv(resultpath+'MACDParameterSet.csv')
    paranum=parasetlist.shape[0]
    # 取合约信息
    symbolInfo = DC.SymbolInfo(symbol)
    # 取源数据
    rawdata_sar = DC.getBarData(symbol, K_MIN_SAR, startdate + " 00:00:00", enddate + " 23:59:59")
    #rawdata_macd = DC.getBarData(symbol, K_MIN_MACD, startdate + " 00:00:00", enddate + " 23:59:59")
    # 取跨合约数据
    contractswaplist = DC.getContractSwaplist(symbol)
    # 计算SAR
    SARlist, reversal = SAR.SAR(rawdata_sar['high'], rawdata_sar['low'], AF_Step, AF_MAX)
    rawdata_sar['SAR'] = SARlist
    rawdata_sar['SAR_R'] = reversal
    rawdata_sar['TR'], rawdata_sar['ATR'] = ATR(rawdata_sar['high'], rawdata_sar['low'], rawdata_sar['close'], ATR_N)
    rawdata_sar['Boll_Mid'], rawdata_sar['Boll_Top'], rawdata_sar['Boll_Bottom'] = BOLL(rawdata_sar['close'], BOLL_N,
                                                                                        BOLL_M, BOLL_P)
    rawdata_sar['boll_filter'] = -1
    rawdata_sar.loc[(rawdata_sar['Boll_Top'] - rawdata_sar['Boll_Mid']) >= 2 * rawdata_sar['ATR'], 'boll_filter'] = 1
    rawdata_sar.to_csv('rawdata_sar.csv')

    # 多进程优化，启动一个对应CPU核心数量的进程池
    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    l = []
    resultlist = pd.DataFrame(columns=
                              ['Setname', 'MACD_S', 'MACD_L', 'MACD_M', 'opentimes', 'end_cash', 'SR', 'Annual','Sharpe','DrawBack',
                               'max_single_loss_rate'])
    for i in range(0, paranum):
        setname = parasetlist.ix[i, 'Setname']
        macd_s = parasetlist.ix[i, 'MACD_Short']
        macd_l = parasetlist.ix[i, 'MACD_Long']
        macd_m = parasetlist.ix[i, 'MACD_M']
        macdParaSet = {
            'MACD_S': macd_s,
            'MACD_L': macd_l,
            'MACD_M': macd_m,
        }
        HopeWin(symbolInfo, setname, K_MIN_SAR, K_MIN_MACD, startdate, enddate, macdParaSet, contractswaplist)
        #l.append(pool.apply_async(HopeWin,(symbolInfo,setname,K_MIN_SAR,K_MIN_MACD,startdate,enddate,macdParaSet,contractswaplist)))
    pool.close()
    pool.join()

    # 显示结果
    i = 0
    for res in l:
        resultlist.loc[i] = res.get()
        i += 1
    print resultlist
    finalresults=("%s %d %d finalresults.csv"%(symbol,K_MIN_SAR,K_MIN_MACD))
    resultlist.to_csv(finalresults)
