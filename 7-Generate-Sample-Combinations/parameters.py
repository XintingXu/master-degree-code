#!/usr/bin/python3
# -*- coding: UTF-8 -*
import json

parameters = dict()
json_file_database = "../CONFIG/database.json"
json_file_samples = "../CONFIG/samples.json"


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

    with open(json_file_samples) as sample_file:
        samples = json.load(sample_file)
        parameters["table_captured"] = samples["tablename_captured"]
        parameters["target_type"] = samples["TYPE"]
        parameters["combinations"] = samples["combinations"]

    return parameters

