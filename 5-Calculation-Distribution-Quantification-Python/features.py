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


# returned result contains non-zero
def pmf_to_list(source_dict: dict):
    result = list()
    for key, value in source_dict.items():
        if value != 0:
            result.append(value)
    result.sort(reverse=True)

    return result


def align_pmf(current_pmf: list, reference_pmf: list):
    max_count = min(len(current_pmf), len(reference_pmf))
    return [current_pmf[0:max_count], reference_pmf[0:max_count]]


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


# the length of cdf list should be equal
def wasserstein_distance_cdf(cdf_list1: list, cdf_list2: list):
    if len(cdf_list1) != len(cdf_list2):
        print("CDF List for wassertein are not equal, exiting")
        return 0
    result = 0.0
    for i in range(len(cdf_list1)):
        result = result + abs(cdf_list1[i] - cdf_list2[i])
    return result


# the length of cdf list should be equal
def energy_distance_cdf(cdf_list1: list, cdf_list2: list):
    if len(cdf_list1) != len(cdf_list2):
        print("CDF List for wassertein are not equal, exiting")
        return 0
    result = 0.0
    for i in range(len(cdf_list1)):
        result = result + pow(2*pow((cdf_list1[i] - cdf_list2[i]), 2), 0.5)
    return result


def features(parameters: dict, source_id: int):
    print("Start Table: {0}".format(source_id))

    db_distribution = pymysql.connect(parameters['hostname'], parameters['username'],
                                      parameters['password'], parameters["database_distribution"],
                                      charset='utf8mb4')
    cursor_distribution = db_distribution.cursor()

    sql = "TRUNCATE `{0}`.`evaluation_{1}`;".format(parameters["database_distribution"], source_id)
    cursor_distribution.execute(sql)

    sql_cols = generate_cols(parameters, "DIS")
    sql = "SELECT `IPDDIS`,`COUNTDIS`,{2} FROM `{0}`.`distribution_{1}` " \
          "WHERE `PARAID`='0' and `SOURCEID`='{1}' LIMIT 1;".format(parameters["database_distribution"], source_id, sql_cols)
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
          "WHERE `PARAID`='0' and `SOURCEID`='{1}' LIMIT 1;".format(parameters["database_distribution"], source_id, sql_cols)
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
          "WHERE `PARAID`='0' and `SOURCEID`='{1}' LIMIT 1;".format(parameters["database_distribution"], source_id, sql_cols)
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDCDF = json_map_to_dict(cursor_result[0])
    referenceCOUNTCDF = json_map_to_dict(cursor_result[1])
    referenceWINCDF = dict()
    columeBegin = 2
    for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
        referenceWINCDF[index] = json_map_to_dict(cursor_result[columeBegin])
        columeBegin = columeBegin + 1

    sql = "SELECT `ID` FROM `{0}`.`distribution_{1}` WHERE" \
          " `ID`!=(SELECT `ID` FROM `{0}`.`distribution_{1}` WHERE `SOURCEID`='{1}' AND `PARAID`='0' LIMIT 1);".format(
        parameters["database_distribution"], source_id)
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchall()
    ids = list()
    for id in cursor_result:
        ids.append(int(id[0]))
    print("Table {0} : {1}".format(source_id, ids))

    for id in ids:
        sql_cols = generate_cols(parameters, "DIS")
        sql = "SELECT `SOURCEID`,`PARAID`,`IPDDIS`,`COUNTDIS`,{3} " \
              "FROM `{0}`.`distribution_{1}` WHERE `ID`='{2}';".format(
              parameters["database_distribution"], source_id, id, sql_cols)
        cursor_distribution.execute(sql)
        cursor_result = cursor_distribution.fetchone()
        sourceID = cursor_result[0]
        paraID = cursor_result[1]
        currentIPDValues = dict_to_dis_list(json_map_to_dict(cursor_result[2]))
        currentCOUNTValues = dict_to_dis_list(json_map_to_dict(cursor_result[3]))
        currentWINValues = dict()
        columeBegin = 4
        for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
            currentWINValues[index] = dict_to_dis_list(json_map_to_dict(cursor_result[columeBegin]))
            columeBegin = columeBegin + 1

        sql_cols = generate_cols(parameters, "PMF")
        sql = "SELECT `IPDPMF`,`COUNTPMF`,{3} FROM `{0}`.`pmf_{1}` WHERE `PARAID`='{2}' AND `SOURCEID`='{4}';".format(
            parameters["database_distribution"], source_id, paraID, sql_cols, sourceID)
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
        sql = "SELECT `IPDCDF`,`COUNTCDF`,{3} FROM `{0}`.`cdf_{1}` WHERE `PARAID`='{2}' AND `SOURCEID`='{4}';".format(
            parameters["database_distribution"], source_id, paraID, sql_cols, sourceID)
        cursor_distribution.execute(sql)
        cursor_result = cursor_distribution.fetchone()
        currentIPDCDF = json_map_to_dict(cursor_result[0])
        currentCOUNTCDF = json_map_to_dict(cursor_result[1])
        currentWINCDF = dict()
        columeBegin = 2
        for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
            currentWINCDF[index] = json_map_to_dict(cursor_result[columeBegin])
            columeBegin = columeBegin + 1

        nonzero_referenceIPDPMF = pmf_to_list(referenceIPDPMF)
        nonzero_currentIPDPMF = pmf_to_list(currentIPDPMF)
        aligned_IPDPMF = align_pmf(nonzero_currentIPDPMF, nonzero_referenceIPDPMF)

        ks_ipd = stats.ks_2samp(referenceIPDValues, currentIPDValues)
        ttest_ipd = stats.ttest_ind(referenceIPDValues, currentIPDValues, axis=0, equal_var=False)
        kld_ipd = stats.entropy(aligned_IPDPMF[0], aligned_IPDPMF[1])
        if math.isinf(kld_ipd):
            kld_ipd = 1.0
        whitney_ipd = stats.mannwhitneyu(referenceIPDValues, currentIPDValues, use_continuity=False, alternative='two-sided')
        ansari_ipd = stats.ansari(referenceIPDValues, currentIPDValues)

        max_ipd_cdf_count = min(len(referenceIPDCDF), len(currentIPDCDF))
        wasserstein_ipd = wasserstein_distance_cdf(cdf_to_list(referenceIPDCDF)[:max_ipd_cdf_count],
                                                     cdf_to_list(currentIPDCDF)[:max_ipd_cdf_count])
        energy_ipd = energy_distance_cdf(cdf_to_list(referenceIPDCDF)[:max_ipd_cdf_count],
                                           cdf_to_list(currentIPDCDF)[:max_ipd_cdf_count])
        print("PARAID = {0} : ".format(paraID), end='\t')
        print("IPD [ks={0},\t ttest={1},\t kld={2},\t whitney={3},\t ansari={4},\t wasserstein={5},\t energy={6}]".format(
            ks_ipd[1], ttest_ipd[1], kld_ipd, whitney_ipd[1], ansari_ipd[1], wasserstein_ipd, energy_ipd), end='\t')

        ansari_count = stats.ansari(referenceCOUNTValues, currentCOUNTValues)

        nonzero_referenceCOUNTPMF = pmf_to_list(referenceCOUNTPMF)
        nonzero_currentCOUNTPMF = pmf_to_list(currentCOUNTPMF)
        aligned_COUNTPMF = align_pmf(nonzero_currentCOUNTPMF, nonzero_referenceCOUNTPMF)
        kld_count = stats.entropy(aligned_COUNTPMF[0], aligned_COUNTPMF[1])
        if math.isinf(kld_count):
            kld_count = 1.0
        ks_count = stats.ks_2samp(referenceCOUNTValues, currentCOUNTValues)
        ttest_count = stats.ttest_ind(referenceCOUNTValues, currentCOUNTValues, axis=0, equal_var=False)
        whitney_count = stats.mannwhitneyu(referenceCOUNTValues, currentCOUNTValues, use_continuity=False, alternative='two-sided')

        aligned_referenceCOUNTCDF = cdf_to_list(referenceCOUNTCDF)
        aligned_currentCOUNTCDF = cdf_to_list(currentCOUNTCDF)
        max_count_cdf = min(len(aligned_referenceCOUNTCDF), len(aligned_currentCOUNTCDF))
        wasserstein_count = wasserstein_distance_cdf(aligned_referenceCOUNTCDF[:max_count_cdf],
                                                     aligned_currentCOUNTCDF[:max_count_cdf])
        energy_count = energy_distance_cdf(aligned_referenceCOUNTCDF[:max_count_cdf],
                                           aligned_currentCOUNTCDF[:max_count_cdf])
        print("COUNT [ks={0},\t ttest={1},\t whitney={2},\t ansari={3},\t kld={4},\t wasserstein={5},\t energy={6}]".format(
            ks_count[1], ttest_count[1], whitney_count[1], ansari_count[1], kld_count, wasserstein_count, energy_count), end='\t')

        kld_win = dict()
        wasserstein_win = dict()
        energy_win = dict()
        for index in range(int(parameters["WIN_BEGIN"]), int(parameters["WIN_END"]) + 1, int(parameters["WIN_STEP"])):
            nonzero_referenceWINCDF = pmf_to_list(referenceWINCDF[index])
            nonzero_currentWINCDF = pmf_to_list(currentWINCDF[index])
            aligned_WINCDF = align_pmf(nonzero_currentWINCDF, nonzero_referenceWINCDF)
            kld_win[index] = stats.entropy(aligned_WINCDF[0], aligned_WINCDF[1])

            # nonzero_referenceWINPMF = pmf_to_list(referenceWINPMF[index])
            # nonzero_currentWINPMF = pmf_to_list(currentWINPMF[index])
            # aligned_WINPMF = align_pmf(nonzero_currentWINPMF, nonzero_referenceWINPMF)
            # kld_win[index] = stats.entropy(aligned_WINPMF[0], aligned_WINPMF[1])

            # kld_win[index] = stats.entropy(cdf_to_list(referenceWINCDF[index]), cdf_to_list(currentWINCDF[index]))
            # wasserstein_win[index] = stats.wasserstein_distance(cdf_to_list(referenceWINCDF[index]), cdf_to_list(currentWINCDF[index]))
            # energy_win[index] = stats.energy_distance(cdf_to_list(referenceWINCDF[index]), cdf_to_list(currentWINCDF[index]))
            # wasserstein_win[index] = stats.wasserstein_distance(pmf_to_list(referenceWINPMF[index]), pmf_to_list(currentWINPMF[index]))
            #energy_win[index] = stats.energy_distance(pmf_to_list(referenceWINPMF[index]), pmf_to_list(currentWINPMF[index]))
            # print("WIN{0} [kld={1},\t wasserstein={2},\t energy={3}]".format(
            #     index, kld_win[index], wasserstein_win[index], energy_win[index]), end='\t')
            wasserstein_win[index] = wasserstein_distance_cdf(cdf_to_list(referenceWINCDF[index]), cdf_to_list(currentWINCDF[index]))
            energy_win[index] = energy_distance_cdf(cdf_to_list(referenceWINCDF[index]), cdf_to_list(currentWINCDF[index]))

        print("")

        col_names = str()
        col_values = str()
        for key, value in kld_win.items():
            col_names = col_names + "`KLD-WIN{0}`,`WASSERSTEIN-WIN{0}`,`ENERGY-WIN{0}`,".format(key)

            if math.isinf(value):
                value = 1
            col_values = col_values + "'{0}','{1}','{2}',".format(value, wasserstein_win[key], energy_win[key])
        sql = "INSERT INTO `{0}`.`evaluation_{1}` " \
              "({2}`KSP-IPD`,`KLD-IPD`,`TTESTP-IPD`,`WHITNEYP-IPD`,`ANSARIP-IPD`,`WASSERSTEIN-IPD`,`ENERGY-IPD`," \
              "`KSP-COUNT`,`KLD-COUNT`,`TTESTP-COUNT`,`WHITNEYP-COUNT`,`ANSARIP-COUNT`,`WASSERSTEIN-COUNT`,`ENERGY-COUNT`," \
              "`SOURCEID`,`PARAID`)" \
              " VALUES({3}'{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}','{15}','{16}','{17}'," \
              "'{18}', '{19}');".format(
            parameters["database_distribution"], source_id, col_names, col_values,
            ks_ipd[1], kld_ipd, ttest_ipd[1], whitney_ipd[1], ansari_ipd[1], wasserstein_ipd, energy_ipd,
            ks_count[1], kld_count, ttest_count[1], whitney_count[1], ansari_count[1], wasserstein_count, energy_count,
            sourceID, paraID)

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

    sql = "SELECT `IPDDIS`,`COUNTDIS` FROM `{0}`.`distribution_31` " \
          "WHERE `PARAID`='0' AND `SOURCEID`='31' LIMIT 1;".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDValues_excellent = dict_to_dis_list(json_map_to_dict(cursor_result[0]))
    referenceCountValues_excellent = dict_to_dis_list(json_map_to_dict(cursor_result[1]))

    sql = "SELECT `IPDDIS`,`COUNTDIS` FROM `{0}`.`distribution_32` " \
          "WHERE `PARAID`='0' AND `SOURCEID`='32' LIMIT 1;".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDValues_good = dict_to_dis_list(json_map_to_dict(cursor_result[0]))
    referenceCountValues_good = dict_to_dis_list(json_map_to_dict(cursor_result[1]))

    sql = "SELECT `IPDDIS`,`COUNTDIS` FROM `{0}`.`distribution_33` " \
          "WHERE `PARAID`='0' AND `SOURCEID`='33' LIMIT 1;".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceIPDValues_all = dict_to_dis_list(json_map_to_dict(cursor_result[0]))
    referenceCountValues_all = dict_to_dis_list(json_map_to_dict(cursor_result[1]))

    ks_ipd_exc_goo = stats.ks_2samp(referenceIPDValues_excellent, referenceIPDValues_good)
    ks_ipd_exc_all = stats.ks_2samp(referenceIPDValues_excellent, referenceIPDValues_all)
    ks_ipd_goo_all = stats.ks_2samp(referenceIPDValues_good, referenceIPDValues_all)

    print("ks-ipd:[exc-goo={0},\texc-all={1},\tgoo-all={2}]".format(ks_ipd_exc_goo,
                                                                    ks_ipd_exc_all, ks_ipd_goo_all))

    ks_count_exc_goo = stats.ks_2samp(referenceCountValues_excellent, referenceCountValues_good)
    ks_count_exc_all = stats.ks_2samp(referenceCountValues_excellent, referenceCountValues_all)
    ks_count_goo_all = stats.ks_2samp(referenceCountValues_good, referenceCountValues_all)

    print("ks-count:[exc-goo={0},\texc-all={1},\tgoo-all={2}]".format(ks_count_exc_goo,
                                                                      ks_count_exc_all, ks_count_goo_all))

    sql = "SELECT `WIN200PMF` FROM `{0}`.`pmf_31` " \
          "WHERE `ID`='1' LIMIT 1;".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceWinPMF_excellent = pmf_to_list(json_map_to_dict(cursor_result[0]))

    sql = "SELECT `WIN200PMF` FROM `{0}`.`pmf_31` " \
          "WHERE `ID`='107' LIMIT 1;".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceWinPMF_excellent2 = pmf_to_list(json_map_to_dict(cursor_result[0]))

    sql = "SELECT `WIN200CDF` FROM `{0}`.`cdf_31` " \
          "WHERE `ID`='1' LIMIT 1;".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceWinCDF_excellent = cdf_to_list(json_map_to_dict(cursor_result[0]))

    sql = "SELECT `WIN200CDF` FROM `{0}`.`cdf_31` " \
          "WHERE `ID`='107' LIMIT 1;".format(parameters["database_distribution"])
    cursor_distribution.execute(sql)
    cursor_result = cursor_distribution.fetchone()
    referenceWinCDF_excellent2 = cdf_to_list(json_map_to_dict(cursor_result[0]))

    pmf_count = min(len(referenceWinPMF_excellent), len(referenceWinPMF_excellent2))
    referenceWinPMF_excellent = referenceWinPMF_excellent[:pmf_count]
    referenceWinPMF_excellent2 = referenceWinPMF_excellent2[:pmf_count]

    was_dis = wasserstein_distance_cdf(referenceWinCDF_excellent, referenceWinCDF_excellent2)
    ene_dis = energy_distance_cdf(referenceWinCDF_excellent, referenceWinCDF_excellent2)

    print("WIN100:[was:{0}\tenergy:{1}]".format(was_dis, ene_dis))

    cursor_distribution.close()
    db_distribution.close()
