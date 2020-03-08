#!/usr/bin/python3
# -*- coding: UTF-8 -*

import numpy as np
from scipy.fftpack import fft
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import pymysql as pymysql
import json


def drop_json_to_list(source_string: str):
    result = list()
    source = json.loads(source_string)
    drop_items = source["dropped"]
    for item in drop_items:
        result.append(item)
    result.sort()
    return result

def generate_sample_list(max_seq: int, drop_list: list):
    result = list()
    for seq in range(1, max_seq + 1):
        if seq in drop_list:
            result.append(1)
        else:
            result.append(0)
    return result

def features(parameters: dict, source_id: int):
    print("Table {0} Start".format(source_id))

    db_source = pymysql.connect(parameters['hostname'], parameters['username'],
                                      parameters['password'], parameters["database_source"],
                                      charset='utf8mb4')
    cursor_source = db_source.cursor()
    db_modulation = pymysql.connect(parameters['hostname'], parameters['username'],
                                parameters['password'], parameters["database_modulation"],
                                charset='utf8mb4')
    cursor_modulation = db_modulation.cursor()

    sql = "SELECT `MAXSEQ`,`LOSTLIST` FROM `{0}`.`{1}` WHERE `ID`='{2}';".format(parameters['database_source'],
                                                                                 parameters['table_captured'],
                                                                                 source_id)
    cursor_source.execute(sql)
    cursor_result = cursor_source.fetchone()
    max_sequence = cursor_result[0]
    dropped_captured = drop_json_to_list(cursor_result[1])

    sample_result = generate_sample_list(max_sequence, dropped_captured)

    yf = abs(fft(sample_result))/max_sequence
    xf = np.arange(max_sequence)

    plt.plot(xf[0:100], yf[0:100], 'g')
    plt.title('FFT of Mixed wave(normalization)', fontsize=9, color='r')

    plt.savefig("{0}.png".format(source_id))
    plt.close()

    print("Table {0} Done".format(source_id))


if __name__ == '__main__':
    #采样点选择1400个，因为设置的信号频率分量最高为600赫兹，根据采样定理知采样频率要大于信号频率2倍，所以这里设置采样频率为1400赫兹（即一秒内有1400个采样点，一样意思的）
    x = np.linspace(0, 1, 200000)

    #设置需要采样的信号，频率分量有180，390和600
    y = 7*np.sin(2*np.pi*200*x) + 2.8*np.sin(2*np.pi*400*x) + 5.1*np.sin(2*np.pi*600*x) + 1.7*np.sin(2*np.pi*99999*x)

    yy = fft(y)                     #快速傅里叶变换
    yreal = yy.real               # 获取实数部分
    yimag = yy.imag               # 获取虚数部分

    yf = abs(fft(y))                # 取绝对值
    yf1 = abs(fft(y))/len(x)           #归一化处理
    yf2 = yf1[range(int(len(x)/2))]  #由于对称性，只取一半区间

    xf = np.arange(len(y))        # 频率
    xf1 = xf
    xf2 = xf[range(int(len(x)/2))]  #取一半区间

    plt.subplot(221)
    plt.plot(x[0:50], y[0:50])
    plt.title('Original wave')

    plt.subplot(222)
    plt.plot(xf, yf, 'r')
    plt.title('FFT of Mixed wave(two sides frequency range)', fontsize=7, color='#7A378B')  #注意这里的颜色可以查询颜色代码表

    plt.subplot(223)
    plt.plot(xf1, yf1, 'g')
    plt.title('FFT of Mixed wave(normalization)', fontsize=9, color='r')

    plt.subplot(224)
    plt.plot(xf2, yf2, 'b')
    plt.title('FFT of Mixed wave)', fontsize=10, color='#F08080')

    plt.savefig("out.png")