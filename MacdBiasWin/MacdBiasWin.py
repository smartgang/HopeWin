# -*- coding: utf-8 -*-
"""
开仓条件：
--------------------------------------------------------------------------------
   MACD金叉 同时  BIAS<      -3      收盘价向下离均线较远时    应该要回归均线     做多
   MACD死叉 同时  BIAS>       3      收盘价向上离均线较远时    应该要回归均线     做空
"""
import MA
from BIAS import bias
import pandas as pd


def MacdBiasWin(symbolinfo, rawdata_macd, macdParaSet):
    setname = macdParaSet['Setname']
    MACD_S = macdParaSet['MACD_S']
    MACD_L = macdParaSet['MACD_L']
    MACD_M = macdParaSet['MACD_M']
    BIAS_N = macdParaSet['BIAS_N']
    BIAS_UP = macdParaSet['BIAS_UP']
    BIAS_DOWN = macdParaSet['BIAS_DOWN']
    rawdata_macd['Unnamed: 0'] = range(rawdata_macd.shape[0])
    # 计算MACD
    #macd = MA.calMACD(rawdata_macd['close'], MACD_S, MACD_L, MACD_M)   # 普通MACD
    macd = MA.hull_macd(rawdata_macd['close'], MACD_S, MACD_L, MACD_M)  # hull_macd
    rawdata_macd['DIF'] = macd[0]
    rawdata_macd['DEA'] = macd[1]
    rawdata_macd['BIAS'] = bias(rawdata_macd['close'], BIAS_N)

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
        (rawdata_macd['MACD_Cross'] == 1) & (rawdata_macd['BIAS'] < BIAS_DOWN)].index
    openshortindex = rawdata_macd.loc[
        (rawdata_macd['MACD_Cross'] == -1) & (rawdata_macd['BIAS'] > BIAS_UP)].index
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
    result = result.dropna()


    slip = symbolinfo.getSlip()
    result['ret'] = ((result['closeprice'] - result['openprice']) * result['tradetype']) - slip
    result['ret_r'] = result['ret'] / result['openprice']
    return result


if __name__ == '__main__':
    pass
