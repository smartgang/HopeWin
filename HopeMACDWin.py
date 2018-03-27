# -*- coding: utf-8 -*-
'''
HopeWin策略纯MACD版本
    多：DIF>0 & （DIF-DEA）>0
    空：DIF<0 & (DIF-DEA)<0
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

def HopeMACDWin(symbolInfo,setname,K_MIN_MACD,startdate,enddate,macdParaSet,contractswaplist,calcResult=True):
    print setname
    rawdata_macd = DC.getBarData(symbolInfo.symbol, K_MIN_MACD, startdate + " 00:00:00", enddate + " 23:59:59")
    MACD_S=macdParaSet['MACD_S']
    MACD_L = macdParaSet['MACD_L']
    MACD_M = macdParaSet['MACD_M']

    macd = MA.calMACD(rawdata_macd['close'], MACD_S, MACD_L, MACD_M)
    rawdata_macd['DIF'] = macd[0]
    rawdata_macd['DEA'] = macd[1]
    rawdata_macd['DIF_F']=-1
    rawdata_macd['DEA_F']=-1
    rawdata_macd.loc[rawdata_macd['DIF']>=0,'DIF_F'] = 1
    rawdata_macd.loc[(rawdata_macd['DIF']>=rawdata_macd['DEA']) , 'DEA_F'] = 1
    rawdata_macd['OpenF']=rawdata_macd['DIF_F']+rawdata_macd['DEA_F']
    rawdata_macd['OpenF1']=rawdata_macd['OpenF'].shift(1).fillna(0)
    # ================================ 找出买卖点================================================
    # 1.OpenF==2 & (OpenF1 ==-2/0):开多
    # 2.OpenF==-2/0& （OpenfF1==2):平多
    # 3.OpenF==-2 & （OpenF1 == 2/0）：开空
    # 4.OpenF==2/0 & OpenF1==-2 :平空
    #开多
    openlonglist = pd.DataFrame({'opentime': rawdata_macd.loc[(rawdata_macd['OpenF'] == 2) & (rawdata_macd['OpenF1'] < 2 ), 'strtime']})
    openlonglist['openutc'] = rawdata_macd.loc[(rawdata_macd['OpenF'] == 2) & (rawdata_macd['OpenF1'] < 2 ), 'utc_time']
    openlonglist['openindex'] = rawdata_macd.loc[(rawdata_macd['OpenF'] == 2) & (rawdata_macd['OpenF1'] < 2 ), 'Unnamed: 0']
    openlonglist['openprice'] = rawdata_macd.loc[(rawdata_macd['OpenF'] == 2) & (rawdata_macd['OpenF1'] < 2 ), 'close']
    # 平多
    closelonglist = pd.DataFrame(
        {'closetime': rawdata_macd.loc[(rawdata_macd['OpenF1'] == 2) & (rawdata_macd['OpenF'] < 2), 'strtime']})
    closelonglist['closeutc'] = rawdata_macd.loc[
        (rawdata_macd['OpenF1'] == 2) & (rawdata_macd['OpenF'] < 2), 'utc_time']
    closelonglist['closeindex'] = rawdata_macd.loc[
        (rawdata_macd['OpenF1'] == 2) & (rawdata_macd['OpenF'] < 2), 'Unnamed: 0']
    closelonglist['closeprice'] = rawdata_macd.loc[
        (rawdata_macd['OpenF1'] == 2) & (rawdata_macd['OpenF'] < 2), 'close']

    #开空
    openshortlist = pd.DataFrame({'opentime': rawdata_macd.loc[(rawdata_macd['OpenF'] == -2) & (rawdata_macd['OpenF1'] > -2 ), 'strtime']})
    openshortlist['openutc'] = rawdata_macd.loc[(rawdata_macd['OpenF'] == -2) & (rawdata_macd['OpenF1'] > -2 ), 'utc_time']
    openshortlist['openindex'] = rawdata_macd.loc[(rawdata_macd['OpenF'] == -2) & (rawdata_macd['OpenF1'] > -2 ), 'Unnamed: 0']
    openshortlist['openprice'] = rawdata_macd.loc[(rawdata_macd['OpenF'] == -2) & (rawdata_macd['OpenF1'] > -2 ), 'close']
    # 平空
    closeshortlist = pd.DataFrame(
        {'closetime': rawdata_macd.loc[(rawdata_macd['OpenF1'] == -2) & (rawdata_macd['OpenF'] > -2), 'strtime']})
    closeshortlist['closeutc'] = rawdata_macd.loc[
        (rawdata_macd['OpenF1'] == -2) & (rawdata_macd['OpenF'] > -2), 'utc_time']
    closeshortlist['closeindex'] = rawdata_macd.loc[
        (rawdata_macd['OpenF1'] == -2) & (rawdata_macd['OpenF'] > -2), 'Unnamed: 0']
    closeshortlist['closeprice'] = rawdata_macd.loc[
        (rawdata_macd['OpenF1'] == -2) & (rawdata_macd['OpenF'] > -2), 'close']

    openlonglist.reset_index(drop=True,inplace=True)
    closelonglist.reset_index(drop=True,inplace=True)
    openshortlist.reset_index(drop=True,inplace=True)
    closeshortlist.reset_index(drop=True,inplace=True)
    # 生成多仓序列（金叉在前，死叉在后）
    if openlonglist.ix[0, 'openindex'] < closelonglist.ix[0, 'closeindex']:
        longlist = pd.concat([openlonglist, closelonglist], axis=1)
    else:  # 如果第一个死叉的序号在金叉前，则要将死叉往上移1格
        longlist = pd.concat([openlonglist, closelonglist.shift(-1).fillna(0)], axis=1)
    #longlist.set_index('openindex', inplace=True)
    longlist['tradetype']=1

    # 生成空仓序列（死叉在前，金叉在后）
    if openshortlist.ix[0, 'openindex'] < closeshortlist.ix[0, 'closeindex']:
        shortlist = pd.concat([openshortlist, closeshortlist], axis=1)
    else:  # 如果第一个金叉的序号在死叉前，则要将金叉往上移1格
        shortlist = pd.concat([openshortlist, closeshortlist.shift(-1).fillna(0)], axis=1)
    #shortlist.set_index('openindex', drop=True)
    shortlist['tradetype']=-1

    # 结果分析
    result = pd.concat([longlist, shortlist])
    result = result.set_index(pd.Index(result['openindex']), drop=True)
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
    filename = ("%s%d %s result.csv" % (symbolInfo.symbol, K_MIN_MACD,setname))
    result.to_csv(filename)
    rawdata_macd.to_csv('macd '+filename)
    del rawdata_macd
    return results

if __name__=='__main__':
    #====================参数和文件夹设置======================================
    #参数设置
    exchange_id = 'DCE'
    sec_id='I'
    K_MIN_MACD = 900
    symbol = '.'.join([exchange_id, sec_id])
    startdate='2017-01-01'
    enddate = '2017-12-31'


    #MACD参数
    MACD_SHORT = 6
    MACD_LONG = 30
    MACD_M = 9

    #文件路径
    upperpath=DC.getUpperPath(1)
    foldername = ' '.join([exchange_id, sec_id,str(K_MIN_MACD)])
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
    # 取跨合约数据
    contractswaplist = DC.getContractSwaplist(symbol)
    swaplist = np.array(contractswaplist.swaputc)

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
        #HopeMACDWin(symbolInfo, setname, K_MIN_MACD, startdate, enddate, macdParaSet, contractswaplist)
        l.append(pool.apply_async(HopeMACDWin,(symbolInfo,setname,K_MIN_MACD,startdate,enddate,macdParaSet,swaplist)))
    pool.close()
    pool.join()

    # 显示结果
    i = 0
    for res in l:
        resultlist.loc[i] = res.get()
        i += 1
    print resultlist
    finalresults=("%s %d finalresults.csv"%(symbol,K_MIN_MACD))
    resultlist.to_csv(finalresults)
