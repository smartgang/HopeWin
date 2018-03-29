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
enddate = '2017-12-31'
parasetname = 'MACDParameterSet1.csv'

#=============止损控制开关===================
calcDsl_close=False
calcOwnl_close=True
calcDslOwnl_close=False
#dsl参数
dslStep_close=-0.002
dslTargetStart_close=-0.010
dslTargetEnd_close = -0.042
#ownl参数
ownlStep_close=0.001
ownlTargetStart_close = 0.008
ownltargetEnd_close = 0.011
nolossThreshhold_close = 3
#=============推进控制开关===================
nextMonthName='Jan-18'
forwardWinStart=1
forwardWinEnd=12

#止损类型开关
common_forward=False #普通回测结果推进
calcDsl_forward=False
calcOwnl_forward=False
calcDslOwnl_forward=False
#dsl参数
dslStep_forward=-0.002
dslTargetStart_forward=-0.010
dslTargetEnd_forward = -0.042
#ownl参数
ownlStep_forward=0.001
ownlTargetStart_forward = 0.005
ownltargetEnd_forward = 0.010
#dsl_ownl set:dsl在前，ownl在后
dsl_ownl_set=[[-0.020,0.009],[-0.018,0.009]]

#============================================
#品种集和周期集，只在多品种多周期优化回测中起作用
symbol_Set=['SHFE.RB','DCE.I']
K_MIN_Set = [600,900,3600]
