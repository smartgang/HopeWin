# -*- coding: utf-8 -*-
'''
MACD数据预处理
需要将1小时的MACD数据按15min的频率进行实时计算，根据参数集组将每一组参数对应的文件保存下来，供HopeWin测试时使用
方法：
先将结束时间相同的对齐
再使用每次计算单个MACD增量的方法
DIFF : EMA(CLOSE,SHORT) - EMA(CLOSE,LONG);
DEA  : EMA(DIFF,M);
2*(DIFF-DEA),COLORSTICK;
'''

import pandas as pd
import os
import DATA_CONSTANTS as DC
import MA
import numpy as np
def prepareMACD(rawdata_sar,rawdata_macd,MACD_S,MACD_L,MACD_M):
    # 计算MACD
    macd = MA.calMACD(rawdata_macd['close'], MACD_S, MACD_L, MACD_M)
    rawdata_macd['DIF'] = macd[0]
    rawdata_macd['DEA'] = macd[1]

    # 将MACD合并到SAR数据
    rawdata_sar.set_index('utc_endtime', inplace=True)
    rawdata_macd.set_index('utc_endtime', inplace=True)
    rawdata_sar['DIF'] = rawdata_macd['DIF']
    rawdata_sar['DEA'] = rawdata_macd['DEA']
    rawdata_sar['close60'] = rawdata_macd['close']
    # rawdata_sar.fillna(method='ffill', inplace=True)
    rawdata_sar.reset_index(inplace=True)

    close15m = rawdata_sar['close']
    close60m = rawdata_sar['close60']
    dif = pd.Series(0)
    dea = pd.Series(0)
    difbuf = pd.Series(0)
    closebuf = pd.Series(0)
    # closebuf.append(close15m[0])
    l = 0
    for i in range(len(close15m)):

        closebuf[l] = close15m[i]  # 用新的15m的close值用来充当60m的close
        sema = closebuf.ewm(span=MACD_S).mean()
        lema = closebuf.ewm(span=MACD_L).mean()
        a = sema[l] - lema[l]
        dif[i] = a
        difbuf[l] = a
        dea[i] = difbuf.ewm(span=MACD_M).mean()[l]
        c = close60m[i]
        if np.isnan(c):
            pass
        else:
            l += 1
    rawdata_sar['dif_new'] = dif
    rawdata_sar['dea_new'] = dea
    #print closebuf
    #rawdata_sar.to_csv('endtimecompolie.csv')
    return rawdata_sar

if __name__ =='__main__':
    exchange_id = 'DCE'
    sec_id = 'I'
    K_MIN_SAR = 900
    K_MIN_MACD = 3600
    symbol = '.'.join([exchange_id, sec_id])

    upperpath=DC.getUpperPath(1)
    os.chdir(upperpath+'\\testdata')
    rawdata_sar=pd.read_csv('sardata.csv')
    #rawdata_macd = DC.getBarData(symbol, K_MIN_MACD, startdate + " 00:00:00", enddate + " 23:59:59")
    rawdata_macd = pd.read_csv('testdata3600.csv')
    MACD_S=12
    MACD_L = 26
    MACD_M = 9
    sar=prepareMACD(rawdata_sar,rawdata_macd,MACD_S,MACD_L,MACD_M)
    sar.to_csv('endtimecompolie.csv')