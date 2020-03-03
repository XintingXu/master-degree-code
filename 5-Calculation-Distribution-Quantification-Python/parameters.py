#!/usr/bin/python3
# -*- coding: UTF-8 -*
import json

parameters = dict()
json_file_database = "../CONFIG/database.json"
json_file_distribution = "../CONFIG/distribution.json"


def get_parameters():
    with open(json_file_database) as database_file:
        database = json.load(database_file)
        parameters["hostname"] = database["hostname"]
        parameters["username"] = database["username"]
        parameters["password"] = database["password"]
        parameters["database_source"] = database["databases"]["source"]
        parameters["database_modulation"] = database["databases"]["modulation"]
        parameters["database_distribution"] = database["databases"]["distribution"]
        parameters["database_evaluation"] = database["databases"]["evaluation"]

    with open(json_file_distribution) as distribution_file:
        distribution = json.load(distribution_file)
        parameters["table_captured"] = distribution["tablename_captured"]
        parameters["type"] = distribution["TYPE"]
        parameters["threads"] = distribution["THREADS"]
        parameters["WIN_BEGIN"] = distribution["WIN_BEGIN"]
        parameters["WIN_END"] = distribution["WIN_END"]
        parameters["WIN_STEP"] = distribution["WIN_STEP"]

    return parameters

