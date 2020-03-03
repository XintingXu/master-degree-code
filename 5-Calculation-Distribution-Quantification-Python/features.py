#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import pymysql as pymysql
from scipy import stats
import json
import parameters
import math


def json_map_to_dict(source_string: str):
    result = dict()
    source = json.loads(source_string)
    for key, value in source.items():
        result[int(key)] = value
    return result


def dict_to_dis_list(source_map: dict):
    result = list()
    for key, value in source_map.items():
        for count in range(0, int(value)):
            result.append(key)
    return result


def pmf_to_list(source_dict: dict, begin: int, end: int):
    result = list()
    for key in range(begin, end + 1):
        if key in source_dict:
            result.append(source_dict[key])
        else:
            result.append(0.0)
    return result


def cdf_to_list(source_dict: dict):
    result = list()
    keys = list(source_dict.keys())
    keys.sort()
    for key in keys:
        result.append(source_dict[key])
    return result


def generate_cols(parameters: dict, name: str):
    sql_cols = str()
    for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]), int(parameters["WIN_STEP"])):
        sql_cols += "`WIN{0}{1}`,".format(index, name)
    sql_cols += "`WIN{0}{1}`".format(parameters["WIN_END"], name)
    return sql_cols


def features(parameters: dict, source_id: int):
    print("Start Table: {0}".format(source_id))

    db_distribution = pymysql.connect(parameters['hostname'], parameters['username'],
                                      parameters['password'], parameters["database_distribution"],
                                      charset='utf8mb4')
    cursor_distribution = db_distribution.cursor()

    sql_cols = generate_cols(parameters, "DIS")
    sql = "SELECT `IPDDIS`,`COUNTDIS`,{2} FROM `{0}`.`distribution_{1}` " \
          "WHERE `PARAID`='0';".format(parameters["database_distribution"], source_id, sql_cols)
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDValues = dict_to_dis_list(json_map_to_dict(cursor_result[0]))
    referenceCOUNTValues = dict_to_dis_list(json_map_to_dict(cursor_result[1]))
    referenceWINValues = dict()
    columeBegin = 2
    for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
        referenceWINValues[index] = dict_to_dis_list(json_map_to_dict(cursor_result[columeBegin]))
        columeBegin = columeBegin + 1

    sql_cols = generate_cols(parameters, "PMF")
    sql = "SELECT `IPDPMF`,`COUNTPMF`,{2} FROM `{0}`.`pmf_{1}` " \
          "WHERE `PARAID`='0';".format(parameters["database_distribution"], source_id, sql_cols)
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDPMF = json_map_to_dict(cursor_result[0])
    referenceCOUNTPMF = json_map_to_dict(cursor_result[1])
    referenceWINPMF = dict()
    columeBegin = 2
    for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
        referenceWINPMF[index] = json_map_to_dict(cursor_result[columeBegin])
        columeBegin = columeBegin + 1

    sql_cols = generate_cols(parameters, "CDF")
    sql = "SELECT `IPDCDF`,`COUNTCDF`,{2} FROM `{0}`.`cdf_{1}` " \
          "WHERE `PARAID`='0';".format(parameters["database_distribution"], source_id, sql_cols)
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDCDF = json_map_to_dict(cursor_result[0])
    referenceCOUNTCDF = json_map_to_dict(cursor_result[1])
    referenceWINCDF = dict()
    columeBegin = 2
    for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
        referenceWINCDF[index] = json_map_to_dict(cursor_result[columeBegin])
        columeBegin = columeBegin + 1

    sql = "SELECT `ID` FROM `{0}`.`distribution_{1}` WHERE `PARAID` > '0';".format(
        parameters["database_distribution"], source_id)
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchall()
    ids = list()
    for id in cursor_result:
        ids.append(int(id[0]))
    print("Table {0} : {1}".format(source_id, ids))

    for id in ids:
        sql_cols = generate_cols(parameters, "DIS")
        sql = "SELECT `PARAID`,`IPDDIS`,`COUNTDIS`,{3} " \
              "FROM `{0}`.`distribution_{1}` WHERE `ID`='{2}'".format(
              parameters["database_distribution"], source_id, id, sql_cols)
        cursor_distribution.execute(sql)
        cursor_result = cursor_distribution.fetchone()
        paraID = cursor_result[0]
        currentIPDValues = dict_to_dis_list(json_map_to_dict(cursor_result[1]))
        currentCOUNTValues = dict_to_dis_list(json_map_to_dict(cursor_result[2]))
        currentWINValues = dict()
        columeBegin = 2
        for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
            currentWINValues[index] = dict_to_dis_list(json_map_to_dict(cursor_result[columeBegin]))
            columeBegin = columeBegin + 1

        sql_cols = generate_cols(parameters, "PMF")
        sql = "SELECT `IPDPMF`,`COUNTPMF`,{3} FROM `{0}`.`pmf_{1}` WHERE `PARAID`='{2}'".format(
            parameters["database_distribution"], source_id, paraID, sql_cols)
        cursor_distribution.execute(sql)
        cursor_result = cursor_distribution.fetchone()
        currentIPDPMF = json_map_to_dict(cursor_result[0])
        currentCOUNTPMF = json_map_to_dict(cursor_result[1])
        currentWINPMF = dict()
        columeBegin = 2
        for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
            currentWINPMF[index] = json_map_to_dict(cursor_result[columeBegin])
            columeBegin = columeBegin + 1

        sql_cols = generate_cols(parameters, "CDF")
        sql = "SELECT `IPDCDF`,`COUNTCDF`,{3} FROM `{0}`.`cdf_{1}` WHERE `PARAID`='{2}'".format(
            parameters["database_distribution"], source_id, paraID, sql_cols)
        cursor_distribution.execute(sql)
        cursor_result = cursor_distribution.fetchone()
        currentIPDCDF = json_map_to_dict(cursor_result[0])
        currentCOUNTCDF = json_map_to_dict(cursor_result[1])
        currentWINCDF = dict()
        columeBegin = 2
        for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
            currentWINCDF[index] = json_map_to_dict(cursor_result[columeBegin])
            columeBegin = columeBegin + 1

        pmf_key_begin_IPD = min(min(referenceIPDPMF.keys()), min(currentIPDPMF.keys()))
        pmf_key_end_IPD = max(max(referenceIPDPMF.keys()), max(currentIPDPMF.keys()))
        aligned_referenceIPDPMF = pmf_to_list(referenceIPDPMF, pmf_key_begin_IPD, pmf_key_end_IPD)
        aligned_currentIPDPMF = pmf_to_list(currentIPDPMF, pmf_key_begin_IPD, pmf_key_end_IPD)

        ks_ipd = stats.ks_2samp(referenceIPDValues, currentIPDValues)
        ttest_ipd = stats.ttest_ind(referenceIPDValues, currentIPDValues, axis=0, equal_var=False)
        kld_ipd = stats.entropy(aligned_referenceIPDPMF, aligned_currentIPDPMF)
        if math.isinf(kld_ipd):
            kld_ipd = 1.0
        whitney_ipd = stats.mannwhitneyu(referenceIPDValues, currentIPDValues, use_continuity=False, alternative='two-sided')
        ansari_ipd = stats.ansari(referenceIPDValues, currentIPDValues)
        wasserstein_ipd = stats.wasserstein_distance(aligned_referenceIPDPMF, aligned_currentIPDPMF)
        energy_ipd = stats.energy_distance(aligned_referenceIPDPMF, aligned_currentIPDPMF)

        print("PARAID = {0} : ".format(paraID), end='\t')
        print("IPD [ks={0},\t ttest={1},\t kld={2},\t whitney={3},\t ansari={4},\t wasserstein={5},\t energy={6}]".format(
            ks_ipd[1], ttest_ipd[1], kld_ipd, whitney_ipd[1], ansari_ipd[1], wasserstein_ipd, energy_ipd), end='\t')

        ansari_count = stats.ansari(referenceCOUNTValues, currentCOUNTValues)
        pmf_key_begin_COUNT = min(min(referenceCOUNTPMF.keys()), min(currentCOUNTPMF.keys()))
        pmf_key_end_COUNT = max(max(max(referenceCOUNTPMF.keys()), max(currentCOUNTPMF.keys())), 100)
        aligned_referenceCOUNTPMF = pmf_to_list(referenceCOUNTPMF, pmf_key_begin_COUNT, pmf_key_end_COUNT)
        aligned_currentCOUNTPMF = pmf_to_list(currentCOUNTPMF, pmf_key_begin_COUNT, pmf_key_end_COUNT)
        kld_count = stats.entropy(aligned_referenceCOUNTPMF, aligned_currentCOUNTPMF)
        if math.isinf(kld_count):
            kld_count = 1.0
        ks_count = stats.ks_2samp(aligned_referenceCOUNTPMF, aligned_currentCOUNTPMF)
        ttest_count = stats.ttest_ind(aligned_referenceCOUNTPMF, aligned_currentCOUNTPMF, axis=0, equal_var=False)
        whitney_count = stats.mannwhitneyu(aligned_referenceCOUNTPMF, aligned_currentCOUNTPMF, use_continuity=False, alternative='two-sided')

        aligned_referenceCOUNTCDF = pmf_to_list(referenceCOUNTCDF, pmf_key_begin_COUNT, pmf_key_end_COUNT)
        aligned_currentCOUNTCDF = pmf_to_list(currentCOUNTCDF, pmf_key_begin_COUNT, pmf_key_end_COUNT)
        wasserstein_count = stats.wasserstein_distance(aligned_referenceCOUNTCDF, aligned_currentCOUNTCDF)
        energy_count = stats.energy_distance(aligned_referenceCOUNTCDF, aligned_currentCOUNTCDF)
        print("COUNT [ks={0},\t ttest={1},\t whitney={2},\t ansari={3},\t kld={4},\t wasserstein={5},\t energy={6}]".format(
            ks_count[1], ttest_count[1], whitney_count[1], ansari_count[1], kld_count, wasserstein_count, energy_count), end='\t')

        kld_win = dict()
        wasserstein_win = dict()
        energy_win = dict()
        for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
            kld_win[index] = stats.entropy(cdf_to_list(referenceWINCDF[index]), cdf_to_list(currentWINCDF[index]))
            wasserstein_win[index] = stats.wasserstein_distance(cdf_to_list(referenceWINCDF[index]), cdf_to_list(currentWINCDF[index]))
            energy_win[index] = stats.energy_distance(cdf_to_list(referenceWINCDF[index]), cdf_to_list(currentWINCDF[index]))
            print("WIN{0} [kld={1},\t wasserstein={2},\t energy={3}]".format(
                index, kld_win[index], wasserstein_win[index], energy_win[index]), end='\t')

        print("")

        col_names = str()
        col_values = str()
        for key in kld_win.keys():
            col_names = col_names + "`KLD-WIN{0}`,`WASSERSTEIN-WIN{0}`,`ENERGY-WIN{0}`,".format(key)

            if math.isinf(kld_win[key]):
                kld_win[key] = 1
            col_values = col_values + "'{0}','{1}','{2}',".format(kld_win[key], wasserstein_win[key], energy_win[key])
        sql = "INSERT INTO `{0}`.`evaluation_{1}` " \
              "({2}`KSP-IPD`,`KLD-IPD`,`TTESTP-IPD`,`WHITNEYP-IPD`,`ANSARIP-IPD`,`WASSERSTEIN-IPD`,`ENERGY-IPD`," \
              "`KSP-COUNT`,`KLD-COUNT`,`TTESTP-COUNT`,`WHITNEYP-COUNT`,`ANSARIP-COUNT`,`WASSERSTEIN-COUNT`,`ENERGY-COUNT`," \
              "`SOURCEID`,`PARAID`)" \
              " VALUES({3}'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}'," \
              "'{1}', '{18}');".format(
            parameters["database_distribution"], source_id, col_names, col_values,
            ks_ipd[1], kld_ipd, ttest_ipd[1], whitney_ipd[1], ansari_ipd[1], wasserstein_ipd, energy_ipd,
            ks_count[1], kld_count, ttest_count[1], whitney_count[1], ansari_count[1], wasserstein_count, energy_count,
            paraID)
        # print(sql)
        cursor_distribution.execute(sql)

    db_distribution.commit()
    cursor_distribution.close()
    db_distribution.close()


if __name__ == '__main__':
    print("Start Test")
    parameters = parameters.get_parameters()
    db_distribution = pymysql.connect(parameters['hostname'], parameters['username'],
                                      parameters['password'], parameters["database_distribution"],
                                      charset='utf8mb4')
    cursor_distribution = db_distribution.cursor()

    sql = "SELECT `IPDDIS`,`WIN100DIS`,`WIN200DIS`,`WIN500DIS` FROM `{0}`.`distribution_31` " \
          "WHERE `PARAID`='0';".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDValues1 = dict_to_dis_list(json_map_to_dict(cursor_result[0]))
    referenceWIN100Values1 = dict_to_dis_list(json_map_to_dict(cursor_result[1]))
    referenceWIN200Values1 = dict_to_dis_list(json_map_to_dict(cursor_result[2]))
    referenceWIN500Values1 = dict_to_dis_list(json_map_to_dict(cursor_result[3]))

    sql = "SELECT `IPDDIS`,`WIN100DIS`,`WIN200DIS`,`WIN500DIS` FROM `{0}`.`distribution_32` " \
          "WHERE `PARAID`='0';".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDValues2 = dict_to_dis_list(json_map_to_dict(cursor_result[0]))
    referenceWIN100Values2 = dict_to_dis_list(json_map_to_dict(cursor_result[1]))
    referenceWIN200Values2 = dict_to_dis_list(json_map_to_dict(cursor_result[2]))
    referenceWIN500Values2 = dict_to_dis_list(json_map_to_dict(cursor_result[3]))

    sql = "SELECT `IPDPMF`,`WIN100PMF`,`WIN200PMF`,`WIN500PMF` FROM `{0}`.`pmf_31` " \
          "WHERE `PARAID`='0';".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDPMF1 = json_map_to_dict(cursor_result[0])
    referenceWIN100PMF1 = json_map_to_dict(cursor_result[1])
    referenceWIN200PMF1 = json_map_to_dict(cursor_result[2])
    referenceWIN500PMF1 = json_map_to_dict(cursor_result[3])

    sql = "SELECT `IPDPMF`,`WIN100PMF`,`WIN200PMF`,`WIN500PMF` FROM `{0}`.`pmf_32` " \
          "WHERE `PARAID`='0';".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDPMF2 = json_map_to_dict(cursor_result[0])
    referenceWIN100PMF2 = json_map_to_dict(cursor_result[1])
    referenceWIN200PMF2 = json_map_to_dict(cursor_result[2])
    referenceWIN500PMF2 = json_map_to_dict(cursor_result[3])

    sql = "SELECT `IPDCDF`,`WIN100CDF`,`WIN200CDF`,`WIN500CDF` FROM `{0}`.`cdf_31` " \
          "WHERE `PARAID`='0';".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDCDF1 = json_map_to_dict(cursor_result[0])
    referenceWIN100CDF1 = json_map_to_dict(cursor_result[1])
    referenceWIN200CDF1 = json_map_to_dict(cursor_result[2])
    referenceWIN500CDF1 = json_map_to_dict(cursor_result[3])

    sql = "SELECT `IPDCDF`,`WIN100CDF`,`WIN200CDF`,`WIN500CDF` FROM `{0}`.`cdf_32` " \
          "WHERE `PARAID`='0';".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDCDF2 = json_map_to_dict(cursor_result[0])
    referenceWIN100CDF2 = json_map_to_dict(cursor_result[1])
    referenceWIN200CDF2 = json_map_to_dict(cursor_result[2])
    referenceWIN500CDF2 = json_map_to_dict(cursor_result[3])

    pmf_key_begin_IPD = min(min(referenceIPDPMF1.keys()), min(referenceIPDPMF2.keys()))
    pmf_key_end_IPD = max(max(max(referenceIPDPMF1.keys()), max(referenceIPDPMF2.keys())), 400)
    aligned_referenceIPDPMF1 = pmf_to_list(referenceIPDPMF1, pmf_key_begin_IPD, pmf_key_end_IPD)
    aligned_referenceIPDPMF2 = pmf_to_list(referenceIPDPMF2, pmf_key_begin_IPD, pmf_key_end_IPD)

    ks_ipd = stats.ks_2samp(referenceIPDValues1, referenceIPDValues2)
    ks_win100 = stats.ks_2samp(referenceWIN100Values1, referenceWIN100Values2)
    ks_win200 = stats.ks_2samp(referenceWIN200Values1, referenceWIN200Values2)
    ks_win500 = stats.ks_2samp(referenceWIN500Values1, referenceWIN500Values2)

    ttest_ipd = stats.ttest_ind(referenceIPDValues1, referenceIPDValues2, axis=0, equal_var=False)
    ttest_win100 = stats.ttest_ind(referenceWIN100Values1, referenceWIN100Values2, axis=0, equal_var=False)
    ttest_win200 = stats.ttest_ind(referenceWIN200Values1, referenceWIN200Values2, axis=0, equal_var=False)
    ttest_win500 = stats.ttest_ind(referenceWIN500Values1, referenceWIN500Values2, axis=0, equal_var=False)

    kld_ipd = stats.entropy(aligned_referenceIPDPMF1, aligned_referenceIPDPMF2)
    kld_win100 = stats.entropy(cdf_to_list(referenceWIN100PMF1), cdf_to_list(referenceWIN100PMF2))
    kld_win200 = stats.entropy(cdf_to_list(referenceWIN200PMF1), cdf_to_list(referenceWIN200PMF2))
    kld_win500 = stats.entropy(cdf_to_list(referenceWIN500PMF1), cdf_to_list(referenceWIN500PMF2))

    wd_ipd = stats.wasserstein_distance(aligned_referenceIPDPMF1, aligned_referenceIPDPMF2)
    wd_win100 = stats.wasserstein_distance(cdf_to_list(referenceWIN100PMF1), cdf_to_list(referenceWIN100PMF2))
    wd_win200 = stats.wasserstein_distance(cdf_to_list(referenceWIN200PMF1), cdf_to_list(referenceWIN200PMF2))
    wd_win500 = stats.wasserstein_distance(cdf_to_list(referenceWIN500PMF1), cdf_to_list(referenceWIN500PMF2))

    ed_ipd = stats.energy_distance(aligned_referenceIPDPMF1, aligned_referenceIPDPMF2)
    ed_win100 = stats.energy_distance(cdf_to_list(referenceWIN100PMF1), cdf_to_list(referenceWIN100PMF2))
    ed_win200 = stats.energy_distance(cdf_to_list(referenceWIN200PMF1), cdf_to_list(referenceWIN200PMF2))
    ed_win500 = stats.energy_distance(cdf_to_list(referenceWIN500PMF1), cdf_to_list(referenceWIN500PMF2))

    whitney_ipd = stats.mannwhitneyu(referenceIPDValues1, referenceIPDValues2)
    whitney_win100 = stats.mannwhitneyu(referenceIPDValues1, referenceIPDValues2)
    whitney_win200 = stats.mannwhitneyu(referenceIPDValues1, referenceIPDValues2)
    whitney_win500 = stats.mannwhitneyu(referenceIPDValues1, referenceIPDValues2)

    ranksums_ipd = stats.ranksums(cdf_to_list(referenceIPDCDF1), cdf_to_list(referenceIPDCDF2))
    ranksums_win100 = stats.ranksums(cdf_to_list(referenceWIN100CDF1), cdf_to_list(referenceWIN100CDF2))
    ranksums_win200 = stats.ranksums(cdf_to_list(referenceWIN200CDF1), cdf_to_list(referenceWIN200CDF2))
    ranksums_win500 = stats.ranksums(cdf_to_list(referenceWIN500CDF1), cdf_to_list(referenceWIN500CDF2))

    wilcoxon_ipd = stats.wilcoxon(aligned_referenceIPDPMF1, aligned_referenceIPDPMF2)
    wilcoxon_win100 = stats.wilcoxon(cdf_to_list(referenceWIN100CDF1), cdf_to_list(referenceWIN100CDF2))
    wilcoxon_win200 = stats.wilcoxon(cdf_to_list(referenceWIN200CDF1), cdf_to_list(referenceWIN200CDF2))
    wilcoxon_win500 = stats.wilcoxon(cdf_to_list(referenceWIN500CDF1), cdf_to_list(referenceWIN500CDF2))

    ansari_ipd = stats.ansari(cdf_to_list(referenceIPDCDF1), cdf_to_list(referenceIPDCDF2))
    ansari_win100 = stats.ansari(cdf_to_list(referenceWIN100CDF1), cdf_to_list(referenceWIN100CDF2))
    ansari_win200 = stats.ansari(cdf_to_list(referenceWIN200CDF1), cdf_to_list(referenceWIN200CDF2))
    ansari_win500 = stats.ansari(cdf_to_list(referenceWIN500CDF1), cdf_to_list(referenceWIN500CDF2))

    print("KS-p[{0}, {1}, {2}, {3}]; T-Test-p[{4}, {5}, {6}, {7}]; KL-d[{8}, {9}, {10}, {11}]; "
          "W-d[{12}, {13}, {14}, {15}]; E-d[{16}, {17}, {18}, {19}]".format(
        ks_ipd[1], ks_win100[1], ks_win200[1], ks_win500[1], ttest_ipd[1], ttest_win100[1], ttest_win200[1],
        ttest_win500[1], kld_ipd, kld_win100, kld_win200, kld_win500, wd_ipd, wd_win100, wd_win200, wd_win500,
        ed_ipd, ed_win100, ed_win200, ed_win500), end='')

    print("Whitney-p[{0}, {1}, {2}, {3}]; ranksum-p[{4}, {5}, {6}, {7}]; wilcoxon-p[{8}, {9}, {10}, {11}]; "
          "ansari-p[{12}, {13}, {14}, {15}]".format(whitney_ipd[1], whitney_win100[1], whitney_win200[1],
            whitney_win500[1], ranksums_ipd[1], ranksums_win100[1], ranksums_win200[1], ranksums_win500[1],
            wilcoxon_ipd[1], wilcoxon_win100[1], wilcoxon_win200[1], wilcoxon_win500[1], ansari_ipd[1],
            ansari_win100[1], ansari_win200[1], ansari_win500[1]))

    ks_1_all = stats.ks_2samp(referenceIPDValues2, referenceIPDValues1)
    ks_2_all = stats.ks_2samp(referenceIPDValues1, referenceIPDValues2)

    ttest_1_all = stats.ttest_ind(referenceIPDValues1 + referenceIPDValues2, referenceIPDValues1, axis=0,
                                  equal_var=False)
    ttest_2_all = stats.ttest_ind(referenceIPDValues1 + referenceIPDValues2, referenceIPDValues2, axis=0,
                                  equal_var=False)

    print("All reference: KS-p[{0}, {1}]; TTEST-p[{2}, {3}]".format(ks_1_all[1], ks_2_all[1],
                                                                    ttest_1_all[1], ttest_2_all[1]))

    cursor_distribution.close()
    db_distribution.close()
