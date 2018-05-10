# -*- coding: utf-8 -*-
'''
策略参数设置
'''
#参数设置
strategyName='HopeMacdMaWin'
exchange_id = 'SHFE'
sec_id='RB'
K_MIN = 600
startdate='2016-01-01'
enddate = '2018-05-01'
parasetname = 'MACDParameterSet1.csv'
positionRatio = 1 #持仓比例
initialCash = 200000 #起始资金

#=============止损控制开关===================
progress_close = False
calcDsl_close=True
calcOwnl_close=True
calcFrsl_close=True
calcMultiSLT_close=False
calcDslOwnl_close=False
#dsl参数
dslStep_close=-0.002
dslTargetStart_close=-0.014
dslTargetEnd_close = -0.024
#ownl参数
ownlStep_close=0.001
ownlTargetStart_close = 0.008
ownltargetEnd_close = 0.011
nolossThreshhold_close = 3
#frsl参数
frslStep_close= -0.001
frslTargetStart_close = -0.010
frslTragetEnd_close = -0.011
#=============推进控制开关===================
#nextMonthName='18-05'
forwardWinStart=1
forwardWinEnd=12

#止损类型开关
multiSTL_forward=True #多止损混合推进开关（忽略common模式）
common_forward=False #普通回测结果推进
calcDsl_forward=False
calcOwnl_forward=False
calsFrsl_forward=True
calcDslOwnl_forward=False
#dsl参数
dslStep_forward=-0.002
dslTargetStart_forward=-0.010
dslTargetEnd_forward = -0.042
#ownl参数
ownlStep_forward=0.001
ownlTargetStart_forward = 0.005
ownltargetEnd_forward = 0.010
#frsl参数
frslStep_forward= -0.001
frslTargetStart_forward = -0.010
frslTragetEnd_forward = -0.011
#dsl_ownl set:dsl在前，ownl在后
dsl_ownl_set=[[-0.020,0.009],[-0.018,0.009]]

#==================每月参数计算=====================
#newmonth='2018-05'#要生成参数的新月份
month_n = 7 #n+x的n值，即往前推多少个月

#=================结果指标开关====================
ResultIndexDic=[
    "OprTimes", #操作次数
    "LongOprTimes",#多操作次数
    "ShortOprTimes",#空操作次数
    "EndCash",  # 最终资金
    "LongOprRate",#多操作占比
    "ShortOprRate",#空操作占比
    "Annual",#年化收益
    "Sharpe",#夏普
    "SR",#成功率
    "LongSR",#多操作成功率
    "ShortSR",#空操作成功率
    "DrawBack",#资金最大回撤
    "MaxSingleEarnRate",#单次最大盈利率
    "MaxSingleLossRate",#单次最大亏损率
    "ProfitLossRate",#盈亏比
    "LongProfitLossRate",#多操作盈亏比
    "ShoartProfitLossRate",#空操作盈亏比
    "MaxSuccessiveEarn",#最大连续盈利次数
    "MaxSuccessiveLoss",#最大连续亏损次数
    "AvgSuccessiveEarn",#平均连续盈利次数
    "AveSuccessiveLoss" #平均连续亏损次数'
]
'''
#下面这个是指标全量，要加减从里面挑
ResultIndexDic=[
    "OprTimes", #操作次数
    "LongOprTimes",#多操作次数
    "ShortOprTimes",#空操作次数
    "EndCash",  # 最终资金
    "LongOprRate",#多操作占比
    "ShortOprRate",#空操作占比
    "Annual",#年化收益
    "Sharpe",#夏普
    "SR",#成功率
    "LongSR",#多操作成功率
    "ShortSR",#空操作成功率
    "DrawBack",#资金最大回撤
    "MaxSingleEarnRate",#单次最大盈利率
    "MaxSingleLossRate",#单次最大亏损率
    "ProfitLossRate",#盈亏比
    "LongProfitLossRate",#多操作盈亏比
    "ShoartProfitLossRate",#空操作盈亏比
    "MaxSuccessiveEarn",#最大连续盈利次数
    "MaxSuccessiveLoss",#最大连续亏损次数
    "AvgSuccessiveEarn",#平均连续盈利次数
    "AveSuccessiveLoss" #平均连续亏损次数'
]
'''
#===============多品种多周期优化参数=============================
#多品种多周期优化开关，打开后代码会从下面标识的文件中导入参数
symbol_KMIN_opt_swtich=False

#1.品种和周期组合文件
symbol_KMIN_set_filename=strategyName+'_symbol_KMIN_set.xlsx'
#2.第一步的结果中挑出满足要求的项，做成双止损组合文件
stoploss_set_filename=strategyName+'_stoploss_set.xlsx'
#3.从第二步的结果中挑出满足要求的项，做推进
forward_set_filename=strategyName+'_forward_set.xlsx'

#====================系统参数==================================
folderLevel = 2
resultFolderName = '\\Results\\'