# -*- coding: utf-8 -*-
"""
策略参数设置
"""
# 参数设置
strategyName = 'HopeMacdMaWin'
exchange_id = 'SHFE'
sec_id = 'RB'
K_MIN = 3600
startdate = '2010-01-01'
enddate = '2018-07-01'
parasetname = 'ParameterSet_MACD_MA.csv'
result_para_dic = {  # 结果计算相关参数
    'positionRatio': 1,  # 持仓比例
    'initialCash': 200000,  # 起始资金
    'remove_polar_switch': False,
    'remove_polar_rate': 0.01
}

strategy_para_dic = {
    "MACD_S": [15, 20, 25, 30, 35],
    "MACD_L": [6, 10, 14],
    "MACD_M": [3, 6, 9],
    "MA_N": [6, 10, 14, 18]
}
# ====================止损控制开关======================
calcDsl_close = True   # dsl动态止损开关
dsl_target_list_close = [-0.018, -0.02, -0.022]

calcOwnl_close = True  # ownl有赚不亏开关
ownl_protect_list_close = [0.008, 0.009, 0.010, 0.011]    # ownl保护触发门限
ownl_floor_list_close = [3]   # ownl地板价：止损线(PT数量）

calcGownl_close = True  # gownl递进式有赚不亏开关
gownl_protect_list_close = [0.007, 0.009, 0.011]  # gownl保护触发门限
gownl_floor_list_close = [-4, -1, 2, 5]   # gownl地板价起始点
gownl_step_list_close = [1, 2]    # gownl地板价递进步伐

calcFrsl_close = False  # frsl固定比例止损开关
frsl_target_list_close = [-0.01, -0.011, -0.012]  # 固定止损比例

calcAtrsl_close = False     # atrsl ATR吊灯和yoyo止损开关
atr_pendant_n_list_close = [5, 8]     # 吊灯atr的n值
atr_pendant_rate_list_close = [1.0, 1.5, 2.0]     # 吊灯atr的最大回撤止损atr比例
atr_yoyo_n_list_close = [8, 16, 30]   # yoyo的atr n值
atr_yoyo_rate_list_close = [1, 1.2, 1.5]      # yoyo的止损atr比例

progress_close = False      # 增量模式开关
calcMultiSLT_close = True  # 混合止损开关

# =============推进控制开关===================
# nextMonthName='18-05'
forwardWinStart = 1
forwardWinEnd = 12

# 止损类型开关
multiSTL_forward = True  # 多止损混合推进开关（忽略common模式）
common_forward = False  # 普通回测结果推进
calcDsl_forward = False   # dsl动态止损开关
dsl_target_list_forward = [-0.018, -0.02, -0.022]

calcOwnl_forward = False  # ownl有赚不亏开关
ownl_protect_list_forward = [0.008, 0.009, 0.010, 0,011]    # ownl保护触发门限
ownl_floor_list_forward = [3]   # ownl地板价：止损线(PT数量）

calcGownl_forward = True  # gownl递进式有赚不亏开关
gownl_protect_list_forward = [0.007, 0.009, 0.011]  # gownl保护触发门限
gownl_floor_list_forward = [-4, -1, 2, 5]   # gownl地板价起始点
gownl_step_list_forward = [1, 2]    # gownl地板价递进步伐

calcFrsl_forward = False  # frsl固定比例止损开关
frsl_target_list_forward = [-0.01, -0.011, -0.012]  # 固定止损比例

calcAtrsl_forward = False     # atrsl ATR吊灯和yoyo止损开关
atr_pendant_n_list_forward = [5, 8]     # 吊灯atr的n值
atr_pendant_rate_list_forward = [1.0, 1.5, 2.0]     # 吊灯atr的最大回撤止损atr比例
atr_yoyo_n_list_forward = [8, 16, 30]   # yoyo的atr n值
atr_yoyo_rate_list_forward = [1, 1.2, 1.5]      # yoyo的止损atr比例

progress_forward = False      # 增量模式开关
calcMultiSLT_forward = False  # 混合止损开关

# ==================每月参数计算=====================
# newmonth='2018-05'#要生成参数的新月份
month_n = 7  # n+x的n值，即往前推多少个月

# =================结果指标开关====================
ResultIndexDic = [
    "OprTimes",  # 操作次数
    "LongOprTimes",  # 多操作次数
    "ShortOprTimes",  # 空操作次数
    "EndCash",  # 最终资金
    "MaxOwnCash",  # 最大期间资金
    "LongOprRate",  # 多操作占比
    "ShortOprRate",  # 空操作占比
    "Annual",  # 年化收益
    "Sharpe",  # 夏普
    "SR",  # 成功率
    "LongSR",  # 多操作成功率
    "ShortSR",  # 空操作成功率
    "DrawBack",  # 资金最大回撤
    "MaxSingleEarnRate",  # 单次最大盈利率
    "MaxSingleLossRate",  # 单次最大亏损率
    "ProfitLossRate",  # 盈亏比
    "LongProfitLossRate",  # 多操作盈亏比
    "ShoartProfitLossRate",  # 空操作盈亏比
    "MaxSuccessiveEarn",  # 最大连续盈利次数
    "MaxSuccessiveLoss",  # 最大连续亏损次数
    "AvgSuccessiveEarn",  # 平均连续盈利次数
    "AveSuccessiveLoss"  # 平均连续亏损次数'
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
# ===============多品种多周期优化参数=============================
# 多品种多周期优化开关，打开后代码会从下面标识的文件中导入参数
symbol_KMIN_opt_swtich = False

# 1.品种和周期组合文件
symbol_KMIN_set_filename = strategyName + '_symbol_KMIN_set.xlsx'
# 2.第一步的结果中挑出满足要求的项，做成双止损组合文件
stoploss_set_filename = strategyName + '_stoploss_set.xlsx'
# 3.从第二步的结果中挑出满足要求的项，做推进
forward_set_filename = strategyName + '_forward_set.xlsx'

# ====================系统参数==================================
folderLevel = 2
resultFolderName = '\\Results\\'


# ===================== 通用功能函数 =========================================
def para_str_to_float(para_str):
    # 功能函数：用于将从多品种多周期文件读取进来的字符串格式的参数列表转换为符点型列表
    para_float_list = []
    if type(para_str) != 'str':
        para_float_list.append(float(para_str))
    else:
        for x in para_str.split(','):
            para_float_list.append(float(x))
    return para_float_list


def para_str_to_int(para_str):
    # 功能函数：用于将从多品种多周期文件读取进来的字符串格式的参数列表转换为符点型列表
    para_float_list = []
    if type(para_str) == 'int':
        para_float_list.append(int(para_str))
    else:
        for x in para_str.split(','):
            para_float_list.append(int(x))
    return para_float_list

def generat_para_file(para_list_dic = None):
    import pandas as pd
    if para_list_dic:
        macd_s_list = para_str_to_int(para_list_dic['MACD_S'])
        macd_l_list = para_str_to_int(para_list_dic['MACD_L'])
        macd_m_list = para_str_to_int(para_list_dic['MACD_M'])
        ma_n_list= para_str_to_int(para_list_dic['MA_N'])
    else:
        macd_s_list = strategy_para_dic['MACD_S']
        macd_l_list = strategy_para_dic['MACD_L']
        macd_m_list = strategy_para_dic['MACD_M']
        ma_n_list = strategy_para_dic['MA_N']
    setlist = []
    i = 0
    for s1 in macd_s_list:
        for m1 in macd_m_list:
            for l1 in macd_l_list:
                for ma_n in ma_n_list:
                    setname = "Set%d MS%d ML%d MM%d MA%d" % (i, s1, m1, l1, ma_n)
                    l = [setname, s1, m1, l1, ma_n]
                    setlist.append(l)
                    i += 1

    setpd = pd.DataFrame(setlist, columns=['Setname','MACD_Short','MACD_Long','MACD_M','MA_N'])
    return setpd
