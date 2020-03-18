#include "parameters.h"

#include <QFile>
#include <QIODevice>
#include <QJsonObject>
#include <QJsonDocument>
#include <QDebug>
#include <QJsonArray>

parameters::parameters(QObject *parent, PARAMETERS_TYPE type) : QObject(parent) {
    this->type_defination = type;
}


bool parameters::get_database(database_info_t *info) {
    if (info == nullptr) {
        return false;
    }

    QString settings = load_from_file(DATABASE);
    if (settings.length() == 0) {
        return false;
    }

    QJsonDocument json_document = QJsonDocument::fromJson(settings.toUtf8());
    if (!json_document.isObject()) {
        return false;
    }
    QJsonObject json_object = json_document.object();

    if (json_object.isEmpty()) {
        return false;
    }

    if (json_object.find("hostname") == json_object.end() ||
        json_object.find("username") == json_object.end() ||
        json_object.find("password") == json_object.end() ||
        json_object.find("databases") == json_object.end()) {

        return false;
    }

    info->password = json_object["password"].toString();
    info->host_name = json_object["hostname"].toString();
    info->user_name = json_object["username"].toString();
    if (!json_object["databases"].isObject()) {
        return false;
    }

    QJsonObject database_names = json_object["databases"].toObject();
    for (auto key : database_names.keys()) {
        info->databases[key] = database_names[key].toString();
    }

    return true;
}

bool parameters::get_other_parameters(parameter_t *parameters) {
    if (parameters == nullptr) {
        return false;
    }

    QString settings = load_from_file(this->type_defination);
    if (settings.length() == 0) {
        return false;
    }

    QJsonDocument json_document = QJsonDocument::fromJson(settings.toUtf8());
    if (!json_document.isObject()) {
        return false;
    }
    QJsonObject json_object = json_document.object();

    if (json_object.isEmpty()) {
        return false;
    }

    for (auto key : json_object.keys()) {
        (*parameters)[key] = json_object[key].toString();
    }
    return true;
}

QString parameters::load_from_file(PARAMETERS_TYPE type){
    QString file_name = get_file_name(type);
    if (file_name.length() == 0) {
        return file_name;
    }

    QFile file(file_name);
    if (!file.exists()) {
        return QString("");
    }

    if (!file.open(QIODevice::ReadOnly | QIODevice::Text)) {
        return QString("");
    }

    QString result = file.readAll();
    file.close();

    return result;
}

bool praseTimeMap(QString timeMapStr, QMap<int, int> &result) {
    result.clear();
    QJsonDocument json_document = QJsonDocument::fromJson(timeMapStr.toUtf8());
    if (!json_document.isObject()) {
        return false;
    }
    QJsonObject json_object = json_document.object();

    if (json_object.isEmpty()) {
        return false;
    }

    for (auto key : json_object.keys()) {
        result[key.toInt()] = json_object[key].toString().toInt();
    }
    return true;
}

bool praseCDF(QString cdfStr, QMap<int, double> &result) {
    result.clear();
    QJsonDocument json_document = QJsonDocument::fromJson(cdfStr.toUtf8());
    if (!json_document.isObject()) {
        return false;
    }
    QJsonObject json_object = json_document.object();

    if (json_object.isEmpty()) {
        return false;
    }

    for (auto key : json_object.keys()) {
        result[key.toInt()] = json_object[key].toDouble();
    }
    return true;
}

bool prasePMF(QString disStr, QMap<int, double> &result) {
    QMap<int, int> disMap;
    QJsonDocument json_document = QJsonDocument::fromJson(disStr.toUtf8());
    if (!json_document.isObject()) {
        return false;
    }
    QJsonObject json_object = json_document.object();

    if (json_object.isEmpty()) {
        return false;
    }

    for (auto key : json_object.keys()) {
        disMap[key.toInt()] = json_object[key].toInt();
    }

    double sum = 0.0;
    for (auto it = disMap.begin() ; it != disMap.end() ; ++it) {
        sum += it.value();
    }

    for (auto it = disMap.begin() ; it != disMap.end() ; ++it) {
        result[it.key()] = double(it.value()) / sum;
    }
    return true;
}

bool praseDropList(QString dropListStr, QList<int> &result) {
    result.clear();
    QJsonDocument json_document = QJsonDocument::fromJson(dropListStr.toUtf8());
    if (!json_document.isObject()) {
        return false;
    }
    QJsonObject json_object = json_document.object();

    if (json_object.isEmpty() || json_object.find("dropped") == json_object.end()) {
        return false;
    }

    QJsonArray json_array = json_object["dropped"].toArray();
    for (auto item : json_array) {
        result.push_back(item.toInt());
    }
    return true;
}

bool praseIntCSVtoList(QString csvStr, QList<int> &result) {
    result.clear();

    QStringList item_strs = csvStr.split(',');
    if (csvStr.length() && !item_strs.length()) {
        return false;
    }

    for (auto item : item_strs) {
        if (item.length()) {
            result.push_back(item.toInt());
        }
    }

    return true;
}

bool praseIntListtoCSV(QList<int> *source, QString &result) {
    result.clear();

    for (auto item : *source) {
        result += QString("%1,").arg(item);
    }

    return true;
}
