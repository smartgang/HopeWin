# -*- coding: utf-8 -*-
'''
进场和出场：
    若1小时周期 MACD  金叉   且 K线>MA30      开仓做多        MACD  死叉      平仓
    若1小时周期 MACD  死叉   且K线<MA30       开仓做空        MACD  金叉      平仓
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

def HopeWin_MACD_MA(symbolinfo,rawdata_macd,macdParaSet):
    setname=macdParaSet['Setname']
    MACD_S=macdParaSet['MACD_S']
    MACD_L = macdParaSet['MACD_L']
    MACD_M = macdParaSet['MACD_M']
    MA_N = macdParaSet['MA_N']
    #print setname
    rawdata_macd['Unnamed: 0'] = range(rawdata_macd.shape[0])
    beginindex = rawdata_macd.ix[0, 'Unnamed: 0']
    # 计算MACD
    #macd = MA.calMACD(rawdata_macd['close'], MACD_S, MACD_L, MACD_M)   # 普通MACD
    macd = MA.hull_macd(rawdata_macd['close'], MACD_S, MACD_L, MACD_M)  # hull_macd
    rawdata_macd['DIF'] = macd[0]
    rawdata_macd['DEA'] = macd[1]
    #rawdata_macd['MA'] = MA.calEMA(rawdata_macd['close'],MA_N)
    rawdata_macd['MA'] = MA.hull_ma(rawdata_macd['close'], MA_N)

    # 计算MACD的金叉和死叉
    rawdata_macd['MACD_True'], rawdata_macd['MACD_Cross'] = MA.dfCross(rawdata_macd, 'DIF', 'DEA')

    # ================================ 找出买卖点================================================
    # 1.先找出SAR金叉的买卖点
    # 2.找到结合判决条件的买点
    # 3.从MA买点中滤出真实买卖点
    # 取出金叉点
    goldcrosslist = pd.DataFrame({'goldcrosstime': rawdata_macd.loc[rawdata_macd['MACD_Cross'] == 1, 'strtime']})
    goldcrosslist['goldcrossutc'] = rawdata_macd.loc[rawdata_macd['MACD_Cross'] == 1, 'utc_time']
    goldcrosslist['goldcrossindex'] = rawdata_macd.loc[rawdata_macd['MACD_Cross'] == 1, 'Unnamed: 0']
    goldcrosslist['goldcrossprice'] = rawdata_macd.loc[rawdata_macd['MACD_Cross'] == 1, 'close']

    # 取出死叉点
    deathcrosslist = pd.DataFrame({'deathcrosstime': rawdata_macd.loc[rawdata_macd['MACD_Cross'] == -1, 'strtime']})
    deathcrosslist['deathcrossutc'] = rawdata_macd.loc[rawdata_macd['MACD_Cross'] == -1, 'utc_time']
    deathcrosslist['deathcrossindex'] = rawdata_macd.loc[rawdata_macd['MACD_Cross'] == -1, 'Unnamed: 0']
    deathcrosslist['deathcrossprice'] = rawdata_macd.loc[rawdata_macd['MACD_Cross'] == -1, 'close']
    goldcrosslist = goldcrosslist.reset_index(drop=True)
    deathcrosslist = deathcrosslist.reset_index(drop=True)

    # 生成多仓序列（金叉在前，死叉在后）
    if goldcrosslist.ix[0, 'goldcrossindex'] < deathcrosslist.ix[0, 'deathcrossindex']:
        longcrosslist = pd.concat([goldcrosslist, deathcrosslist], axis=1)
    else:  # 如果第一个死叉的序号在金叉前，则要将死叉往上移1格
        longcrosslist = pd.concat([goldcrosslist, deathcrosslist.shift(-1)], axis=1)
    longcrosslist = longcrosslist.set_index(pd.Index(longcrosslist['goldcrossindex']), drop=True)

    # 生成空仓序列（死叉在前，金叉在后）
    if deathcrosslist.ix[0, 'deathcrossindex'] < goldcrosslist.ix[0, 'goldcrossindex']:
        shortcrosslist = pd.concat([deathcrosslist, goldcrosslist], axis=1)
    else:  # 如果第一个金叉的序号在死叉前，则要将金叉往上移1格
        shortcrosslist = pd.concat([deathcrosslist, goldcrosslist.shift(-1)], axis=1)
    shortcrosslist = shortcrosslist.set_index(pd.Index(shortcrosslist['deathcrossindex']), drop=True)

    # 取出开多序号和开空序号
    openlongindex = rawdata_macd.loc[
            (rawdata_macd['MACD_Cross'] == 1)  & (rawdata_macd['close'] > rawdata_macd['MA']) ].index
    openshortindex = rawdata_macd.loc[
            (rawdata_macd['MACD_Cross'] == -1)  & (rawdata_macd['close'] < rawdata_macd['MA'])].index
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
    #result.drop(result.shape[0] - 1, inplace=True)
    result = result.dropna()
    # 去掉跨合约的操作
    # 使用单合约，不用再去掉跨合约
    #result = removeContractSwap(result, contractswaplist)

    slip = symbolinfo.getSlip()
    result['ret'] = ((result['closeprice'] - result['openprice']) * result['tradetype']) - slip
    result['ret_r'] = result['ret'] / result['openprice']
    results = {}

    '''
    # 使用单合约，策略核心内不再计算结果
    if calcResult:
        result['commission_fee'], result['per earn'], result['own cash'], result['hands'] = RS.calcResult(result,
                                                                                                          symbolinfo,
                                                                                                          initialCash,
                                                                                                          positionRatio)
    
        endcash = result['own cash'].iloc[-1]
        Annual = RS.annual_return(result)
        Sharpe = RS.sharpe_ratio(result)
        DrawBack = RS.max_drawback(result)[0]
        SR = RS.success_rate(result)
        max_single_loss_rate = abs(result['ret_r'].min())

        results = {
            'Setname':setname,
            'opentimes': result.shape[0],
            'end_cash': endcash,
            'SR': SR,
            'Annual': Annual,
            'Sharpe': Sharpe,
            'DrawBack': DrawBack,
            'max_single_loss_rate': max_single_loss_rate
        }
    closeopr = result.loc[:, 'closetime':'tradetype']
    return result, rawdata_macd, closeopr, results
    '''
    return result

if __name__=='__main__':
    #====================参数和文件夹设置======================================
    #参数设置
    strategyName='Hope_MACD_MA'
    exchange_id = 'SHFE'
    sec_id='RB'
    K_MIN = 1800
    symbol = '.'.join([exchange_id, sec_id])
    startdate='2010-01-01'
    enddate = '2017-12-31'

    #MACD参数
    MACD_SHORT = 6
    MACD_LONG = 30
    MACD_M = 9
    #MA参数
    MA_N=30

    #文件路径
    upperpath=DC.getUpperPath(2)
    foldername = ' '.join([strategyName,exchange_id, sec_id, str(K_MIN)])
    resultpath=upperpath+"\\Results\\"
    os.chdir(resultpath)
    try:
        os.mkdir(foldername)
    except:
        print ("%s folder already exsist!" %foldername)
    os.chdir(foldername)

    # ======================数据准备==============================================
    #取参数集
    parasetlist=pd.read_csv(resultpath+'MACDParameterSet2.csv')
    paranum=parasetlist.shape[0]
    # 取合约信息
    symbolInfo = DC.SymbolInfo(symbol)
    # 取源数据
    #rawdata_sar = DC.getBarData(symbol, K_MIN_SAR, startdate + " 00:00:00", enddate + " 23:59:59")
    rawdata_macd = DC.getBarData(symbol, K_MIN, startdate + " 00:00:00", enddate + " 23:59:59")
    # 取跨合约数据
    contractswaplist = DC.getContractSwaplist(symbol)
    swaplist = np.array(contractswaplist.swaputc)

    # 多进程优化，启动一个对应CPU核心数量的进程池
    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    l = []
    resultlist = pd.DataFrame(columns=
                              ['Setname', 'opentimes', 'end_cash', 'SR', 'Annual','Sharpe','DrawBack',
                               'max_single_loss_rate'])
    for i in range(0, paranum):
        setname = parasetlist.ix[i, 'Setname']
        macd_s = parasetlist.ix[i, 'MACD_Short']
        macd_l = parasetlist.ix[i, 'MACD_Long']
        macd_m = parasetlist.ix[i, 'MACD_M']
        ma_n = parasetlist.ix[i,'MA_N']
        macdParaSet = {
            'Setname':setname,
            'MACD_S': macd_s,
            'MACD_L': macd_l,
            'MACD_M': macd_m,
            'MA_N':ma_n
        }
        #HopeWin(symbolInfo, setname, K_MIN_SAR, K_MIN_MACD, startdate, enddate, macdParaSet, contractswaplist)
        l.append(pool.apply_async(HopeWin_MACD_MA,(symbolInfo,K_MIN,startdate,enddate,macdParaSet,swaplist)))
    pool.close()
    pool.join()

    # 显示结果
    i = 0
    for res in l:
        resultlist.loc[i] = res.get()
        i += 1
    print resultlist
    finalresults=("%s %d finalresults.csv"%(symbol,K_MIN))
    resultlist.to_csv(finalresults)
