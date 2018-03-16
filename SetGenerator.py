# -*- coding: utf-8 -*-

import pandas as pd

def parasetGenerator():
    setlist=[]
    i=0
    for ms in range(3,16,1):
        for ml in range(16,36,2):
            for mm in range(3,16,2):
                setname='Set'+str(i)+' MS'+str(ms)+' ML'+str(ml)+' MM'+str(mm)
                l=[setname,ms,ml, mm]
                setlist.append(l)
                i+=1

    setpd=pd.DataFrame(setlist,columns=['Setname','MACD_Short','MACD_Long','MACD_M'])
    setpd.to_csv('MACDParameterSet.csv')

def rankwinSetGenerator():
    setlist=[]
    rankset=range(1,8,1)
    winset=range(1,13,1)
    for r in rankset:
        for w in winset:
            setname=("_Rank%d_win%d_oprResult"%(r,w))
            setlist.append(setname)
    setpd=pd.DataFrame(setlist,columns=['Setname'])
    setpd.to_csv('RankWinSet.csv')

if __name__ == '__main__':
    parasetGenerator()
    #rankwinSetGenerator()