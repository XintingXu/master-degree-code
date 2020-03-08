#!/usr/bin/python3
# -*- coding: UTF-8 -*

import pymysql as pymysql
import feature as features
import parameters

if __name__ == '__main__':
    parameter_info = parameters.get_parameters()
    for key, value in parameter_info.items():
        print("[" + key + "] : " + value)

    db_source = pymysql.connect(parameter_info['hostname'], parameter_info['username'],
                                parameter_info['password'], parameter_info["database_source"],
                                charset='utf8mb4')
    cursor_source = db_source.cursor()

    sql = "SELECT `ID` FROM `{0}`.`{1}` WHERE ".format(parameter_info["database_source"],
                                                       parameter_info["table_captured"])
    where_info = ""
    types = str(parameter_info["type"]).split(',')
    for index in range(0, len(types)):
        where_info += "`TYPE`='{0}'".format(types[index])
        if index is not len(types) - 1:
            where_info += " OR "
    sql += where_info + ";"
    print(sql)

    cursor_source.execute(sql)
    IDs = list()
    temp_result = cursor_source.fetchall()

    cursor_source.close()
    db_source.close()

    for item in temp_result:
        IDs.append(item[0])
        print("Get ID : {0}".format(IDs[len(IDs) - 1]))

    # with ProcessPoolExecutor(int(parameter_info["threads"])) as process_executor:
    #     for source_id in IDs:
    #         process_executor.submit(features.features, parameter_info, int(source_id))

    for source_id in IDs:
        features.features(parameter_info, int(source_id))

    print("All finished.")