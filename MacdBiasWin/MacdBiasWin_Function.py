# -*- coding: utf-8 -*-

import pandas as pd
import DATA_CONSTANTS as DC
import MacdBiasWin_Parameter as Parameter


def set_generator():
    setlist=[]
    i=0
    for ms in range(5,14,1):
        for ml in range(16,29,2):
            for mm in range(5,16,2):
                setname='Set'+str(i)+' MS'+str(ms)+' ML'+str(ml)+' MM'+str(mm)
                l=[setname,ms,ml, mm]
                setlist.append(l)
                i+=1

    setpd=pd.DataFrame(setlist,columns=['Setname','MACD_Short','MACD_Long','MACD_M'])
    setpd['BIAS_N'] = 40
    setpd['BIAS_UP'] = 3
    setpd['BIAS_DOWN'] = -3

    upperpath = DC.getUpperPath(Parameter.folderLevel)
    resultpath = upperpath + Parameter.resultFolderName
    setpd.to_csv(resultpath + Parameter.parasetname)


if __name__=="__main__":
    set_generator()
    pass