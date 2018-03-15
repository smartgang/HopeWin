# -*- coding: utf-8 -*-
'''
SAR  抛物线转向
SAR   位于模块：  talib.func:
SAR(...)
     SAR(high, low[, acceleration=?, maximum=?])
     Parabolic SAR (Overlap Studies)
【输入】
                prices: ['high', 'low']
【参数】
                acceleration: 0.02
                maximum: 0.2
【输出】        real

原理：
SAR(N,STEP,MAX) 返回抛物转向值。

根据公式SAR(n)=SAR(n-1)+AF*(EP(n-1)-SAR(n-1))计算

其中：
SAR(n-1)：上根K线SAR的绝对值
AF：加速因子，当AF小于MAX时，逐根的通过AF+STEP累加，涨跌发生转换时，AF重新计算
EP：一个涨跌内的极值，在上涨行情中为上根K线的最高价；下跌行情中为上根K线的最低价

注：
1、参数N,Step,Max均不支持变量

例1：
SAR(17,0.03,0.3);//表示计算17个周期抛物转向，步长为3%，极限值为30%

    1. 先选定一段时期价格趋势为涨势或跌势。
    2. 这一段时期若为涨势,则选此期间的最低价为第一天的SAR值；若为跌势,则选最高价为第一
天的SAR值。
    3. 以后各天的SAR值计算公式为：:第T日停损点　 :第T-1日停损点　EP:极点值(涨为最高价,
跌为最低价):加速因子
    4.加速因子的取值。上涨行情中,第一次计算SAR时取 为0.02,以后计算SAR,若当日行情价格
有创新高价 加0.02,若无新高价, 为前一天数值,最大不能超过0.2。下跌行情同理。
    5. 看涨期间,计算出某日的SAR比当日或前一日的最低价高,则以当日或前一日的最低价为某日
的SAR；若是看跌期间,某日的SAR比当日或前一日的最高价低,则以当日或前一日最高价为SAR值。
用法：
    1.向下跌破转向点，卖出；向上突破转向点，买入
    2.市道趋势明显时，转向点信号准确，与DMI指标同时使用更好
    3.盘局时，失误率较高
'''
import talib
import pandas as pd
import numpy as np

def SAR(high,low,AF_Step,MAF):
    markettype=1
    high=high.tolist()
    low=low.tolist()
    reversal=[]
    SARlist=[]
    SARlist.append(low[0])
    reversal.append(0)
    AF=0
    lasti=0
    reverseFlag=0
    datanum=len(high)
    for i in range(1,datanum):
        if markettype==1:
            EP=high[i-1]
            AF=min(MAF,AF+AF_Step)
            sar=SARlist[-1]+AF*(EP-SARlist[-1])
            if low[i]<sar:#翻转：涨-》跌
                AF=0
                sar=max(high[lasti:i+1])
                lasti=i
                markettype=-1
                reverseFlag=-1
            else:
                reverseFlag=0
            SARlist.append(sar)
            reversal.append(reverseFlag)
        else:
            EP=low[i-1]
            AF=min(MAF,AF+AF_Step)
            sar=SARlist[-1]+AF*(EP-SARlist[-1])
            if high[i]>sar:#翻转：跌-》涨
                AF=0
                sar=min(low[lasti:i+1])
                lasti=i
                markettype=1
                reverseFlag=1
            else:
                reverseFlag=0
            SARlist.append(sar)
            reversal.append(reverseFlag)
    return SARlist,reversal

if __name__=='__main__':
    testdata=pd.read_csv('testdata\\testdata.csv')
    datanum=testdata.shape[0]

    SARlist,reversal=SAR(testdata['high'],testdata['low'],0.02,0.2)

    testdata['SAR'] = SARlist
    testdata['SAR_R'] = reversal
    testdata.to_csv('testdata\\sardata.csv')