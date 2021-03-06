# -*- coding: utf-8 -*-
'''
进场和出场：
    若1小时周期 MACD  金叉   且 K线>MA30      开仓做多        MACD  死叉      平仓
    若1小时周期 MACD  死叉   且K线<MA30       开仓做空        MACD  金叉      平仓
进场过滤：
    ATR>6

要求：
    传入的rawdata已经带有ATR列
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



def IntradayFryWin(symbolinfo, rawdata, paraSet):
    setname = paraSet['Setname']
    MACD_S = paraSet['MACD_S']
    MACD_L = paraSet['MACD_L']
    MACD_M = paraSet['MACD_M']
    MA_N = paraSet['MA_N']
    # print setname
    rawdata['Unnamed: 0'] = range(rawdata.shape[0])
    #beginindex = rawdata.ix[0, 'Unnamed: 0']
    # 计算MACD
    macd = MA.calMACD(rawdata['close'], MACD_S, MACD_L, MACD_M)
    rawdata['DIF'] = macd[0]
    rawdata['DEA'] = macd[1]
    rawdata['MA'] = MA.calEMA(rawdata['close'], MA_N)

    # 计算MACD的金叉和死叉
    rawdata['MACD_True'], rawdata['MACD_Cross'] = MA.dfCross(rawdata, 'DIF', 'DEA')

    # ================================ 找出买卖点================================================
    # 1.先找出SAR金叉的买卖点
    # 2.找到结合判决条件的买点
    # 3.从MA买点中滤出真实买卖点
    # 取出金叉点
    goldcrosslist = pd.DataFrame({'goldcrosstime': rawdata.loc[rawdata['MACD_Cross'] == 1, 'strtime']})
    goldcrosslist['goldcrossutc'] = rawdata.loc[rawdata['MACD_Cross'] == 1, 'utc_time']
    goldcrosslist['goldcrossindex'] = rawdata.loc[rawdata['MACD_Cross'] == 1, 'Unnamed: 0']
    goldcrosslist['goldcrossprice'] = rawdata.loc[rawdata['MACD_Cross'] == 1, 'close']

    # 取出死叉点
    deathcrosslist = pd.DataFrame({'deathcrosstime': rawdata.loc[rawdata['MACD_Cross'] == -1, 'strtime']})
    deathcrosslist['deathcrossutc'] = rawdata.loc[rawdata['MACD_Cross'] == -1, 'utc_time']
    deathcrosslist['deathcrossindex'] = rawdata.loc[rawdata['MACD_Cross'] == -1, 'Unnamed: 0']
    deathcrosslist['deathcrossprice'] = rawdata.loc[rawdata['MACD_Cross'] == -1, 'close']
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
    openlongindex = rawdata.loc[
        (rawdata['MACD_Cross'] == 1) & (rawdata['close'] > rawdata['MA']) & (rawdata['ATR'] > 6)].index
    openshortindex = rawdata.loc[
        (rawdata['MACD_Cross'] == -1) & (rawdata['close'] < rawdata['MA']) & (rawdata['ATR'] > 6)].index
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
    # result.drop(result.shape[0] - 1, inplace=True)
    result = result.dropna()
    # 去掉跨合约的操作
    # 使用单合约，不用再去掉跨合约
    # result = removeContractSwap(result, contractswaplist)

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
    return result, rawdata, closeopr, results
    '''
    return result


if __name__ == '__main__':
    # ====================参数和文件夹设置======================================
    # 参数设置
    strategyName = 'Hope_MACD_MA'
    exchange_id = 'DCE'
    sec_id = 'I'
    K_MIN = 3600
    symbol = '.'.join([exchange_id, sec_id])
    startdate = '2016-01-01'
    enddate = '2017-12-31'

    # MACD参数
    MACD_SHORT = 6
    MACD_LONG = 30
    MACD_M = 9
    # MA参数
    MA_N = 30

    # 文件路径
    upperpath = DC.getUpperPath(2)
    foldername = ' '.join([strategyName, exchange_id, sec_id, str(K_MIN)])
    resultpath = upperpath + "\\Results\\"
    os.chdir(resultpath)
    try:
        os.mkdir(foldername)
    except:
        print ("%s folder already exsist!" % foldername)
    os.chdir(foldername)

    # ======================数据准备==============================================
    # 取参数集
    parasetlist = pd.read_csv(resultpath + 'MACDParameterSet1.csv')
    paranum = parasetlist.shape[0]
    # 取合约信息
    symbolInfo = DC.SymbolInfo(symbol)
    # 取源数据
    # rawdata_sar = DC.getBarData(symbol, K_MIN_SAR, startdate + " 00:00:00", enddate + " 23:59:59")
    rawdata = DC.getBarData(symbol, K_MIN, startdate + " 00:00:00", enddate + " 23:59:59")
    # 取跨合约数据
    contractswaplist = DC.getContractSwaplist(symbol)
    swaplist = np.array(contractswaplist.swaputc)

    # 多进程优化，启动一个对应CPU核心数量的进程池
    pool = multiprocessing.Pool(multiprocessing.cpu_count() - 1)
    l = []
    resultlist = pd.DataFrame(columns=
                              ['Setname', 'opentimes', 'end_cash', 'SR', 'Annual', 'Sharpe', 'DrawBack',
                               'max_single_loss_rate'])
    for i in range(0, paranum):
        setname = parasetlist.ix[i, 'Setname']
        macd_s = parasetlist.ix[i, 'MACD_Short']
        macd_l = parasetlist.ix[i, 'MACD_Long']
        macd_m = parasetlist.ix[i, 'MACD_M']
        ma_n = parasetlist.ix[i, 'MA_N']
        macdParaSet = {
            'Setname': setname,
            'MACD_S': macd_s,
            'MACD_L': macd_l,
            'MACD_M': macd_m,
            'MA_N': ma_n
        }
        # HopeWin(symbolInfo, setname, K_MIN_SAR, K_MIN_MACD, startdate, enddate, macdParaSet, contractswaplist)
        l.append(pool.apply_async(HopeWin_MACD_MA, (symbolInfo, K_MIN, startdate, enddate, macdParaSet, swaplist)))
    pool.close()
    pool.join()

    # 显示结果
    i = 0
    for res in l:
        resultlist.loc[i] = res.get()
        i += 1
    print resultlist
    finalresults = ("%s %d finalresults.csv" % (symbol, K_MIN))
    resultlist.to_csv(finalresults)
