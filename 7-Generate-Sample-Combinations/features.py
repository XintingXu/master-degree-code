#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import pymysql as pymysql
from scipy import stats
import json
import parameters
import math
import random

buffered_items = dict()


def load_item(cursor_source, parameters: dict, ssrc: str):
    sql = "SELECT `MAXSEQ`,`TIMEMAP`,`LOSTLIST` FROM `{0}`.`{1}` WHERE `INFO`='SSRC={2}';".format(
        parameters["database_source"], parameters["table_captured"], ssrc)
    if ssrc not in buffered_items.keys():
        cursor_source.execute(sql)
        cursor_result = cursor_source.fetchone()
        item = dict()
        item["maxseq"] = cursor_result[0]
        item["timemap"] = json.loads(cursor_result[1])
        item["lostlist"] = json.loads(cursor_result[2])["dropped"]
        buffered_items[ssrc] = item


def features(parameters: dict):
    db_source = pymysql.connect(parameters['hostname'], parameters['username'],
                                      parameters['password'], parameters["database_source"],
                                      charset='utf8mb4')
    cursor_source = db_source.cursor()
    samples = parameters["combinations"]

    for sample in samples:
        max_sequence = 0
        max_timestamp = 0
        timemap = dict()
        lostlist = list()

        items = str(sample).split(',')

        print(items)
        for item in items:
            load_item(cursor_source, parameters, item)

            item_max_seq = buffered_items[item]["maxseq"]
            item_time_map_items = buffered_items[item]["timemap"]
            item_lost_list_items = buffered_items[item]["lostlist"]

            for pkt, time in item_time_map_items.items():
                timemap[str(int(pkt) + max_sequence)] = str(int(time) + max_timestamp)

            for pkt in item_lost_list_items:
                lostlist.append(int(pkt) + max_sequence)

            max_sequence = max_sequence + item_max_seq
            max_timestamp = max_timestamp + int(item_time_map_items[str(item_max_seq)])

        ssrc = random.randint(732270373, 2120598851)
        ssrc_str = str(hex(ssrc))

        timemap_str = json.dumps(timemap, sort_keys=True, separators=(',', ':'))
        droplist_dict = dict()
        droplist_dict["dropped"] = lostlist
        lostlist_str = json.dumps(droplist_dict, sort_keys=True, separators=(',', ':'))

        sql = "INSERT INTO `{0}`.`{1}`(`TYPE`,`INFO`,`MAXSEQ`,`SSRC`,`TIMEMAP`,`LOSTLIST`) VALUES " \
              "('{2}','SSRC={3}','{4}','{5}','{6}','{7}');".format(parameters["database_source"],
                parameters["table_captured"], parameters["target_type"], ssrc_str, max_sequence, ssrc, timemap_str,
                lostlist_str)
        cursor_source.execute(sql)

    db_source.commit()
    cursor_source.close()
    db_source.close()
