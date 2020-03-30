#!/usr/bin/python3
# -*- coding: UTF-8 -*

from concurrent.futures import ProcessPoolExecutor
import pymysql as pymysql
import features as features
import parameters

if __name__ == '__main__':
    parameter_info = parameters.get_parameters()
    for key, value in parameter_info.items():
        if key != "combinations":
            print("[" + key + "] : " + value)

    features.features(parameter_info)

    print("All finished.")
