#ifndef PARAMETERS_H
#define PARAMETERS_H

#include <QObject>
#include <QString>
#include <QMap>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonArray>

typedef struct DATABASE_INFO {
    QString host_name;
    QString user_name;
    QString password;
    QMap<QString, QString> databases;
} database_info_t;

typedef QMap<QString, QString> parameter_t;

enum PARAMETERS_TYPE {
    DATABASE,
    COVERT_MESSAGE,
    MODULATION,
    PARAMETERS_GENERATION,
    RANDOM_NOISE_GENERATION,
    DISTRIBUTION_FEATURE
};

static QString database_fie("../CONFIG/database.json");
static QString covert_message_file("../CONFIG/covertmessage.json");
static QString modulation_file("../CONFIG/modulation.json");
static QString parameters_file("../CONFIG/parameters.json");
static QString noise_generation_file("../CONFIG/generatenoise.json");
static QString distribution_feature_file("../CONFIG/distribution.json");

static QString get_file_name(PARAMETERS_TYPE type) {
    switch(type) {
        case DATABASE:
            return database_fie;
        case COVERT_MESSAGE:
            return covert_message_file;
        case MODULATION:
            return modulation_file;
        case PARAMETERS_GENERATION:
            return parameters_file;
        case RANDOM_NOISE_GENERATION:
            return noise_generation_file;
        case DISTRIBUTION_FEATURE:
            return distribution_feature_file;
    }
    return QString("");
}

bool praseTimeMap(QString timeMapStr, QMap<int, int> &result);
bool praseDropList(QString dropListStr, QList<int> &result);
bool praseCDF(QString cdfStr, QMap<int, double> &result);
bool prasePMF(QString disStr, QMap<int, double> &result);

bool praseIntCSVtoList(QString csvStr, QList<int> &result);

template<typename T>
QString praseMapToJSON(QMap<int, T> *source) {
    QString result;
    QJsonObject json_object;
    for (auto it = source->begin() ; it != source->end() ; ++it) {
        json_object.insert(QString::number(it.key()), QJsonValue(it.value()));
    }
    QJsonDocument json_document(json_object);
    result = json_document.toJson(QJsonDocument::Compact);

    return result;
}

template<typename T>
QString praseListToCSV(QList<T> *source) {
    QString result;

    for (auto item : *source) {
        result += QString(item);
        if (source->indexOf(item) != source->size() - 1) {
            result += ",";
        }
    }

    return result;
}

class parameters : public QObject {
    Q_OBJECT
public:
    explicit parameters(QObject *parent = nullptr, PARAMETERS_TYPE type = COVERT_MESSAGE);

    bool get_database(database_info_t *info);
    bool get_other_parameters(parameter_t *parameters);

private:
    PARAMETERS_TYPE type_defination;

    QString load_from_file(PARAMETERS_TYPE type);
};

#endif // PARAMETERS_H
