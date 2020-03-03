#include "processoperation.h"

#include <QDateTime>
#include <QString>
#include <QThread>
#include <QDebug>
#include <QJsonObject>
#include <QJsonDocument>
#include <QJsonArray>
#include <QSqlError>

QFile *ProcessOperation::logFile;
QTextStream ProcessOperation::Log;
volatile int ProcessOperation::lineNumber;
QMutex ProcessOperation::LogMutex;

ProcessOperation::ProcessOperation(QObject *parent, database_info_t *database_info,
                                   parameter_t *parameters) : QObject(parent) {
    this->database = *database_info;
    this->parameter = *parameters;
    lineNumber = 1;
    logFile = nullptr;

    logFile = new QFile("ProcessLog_" +
                        QDateTime::currentDateTime().toLocalTime().toString("yyyy-MM-dd_hh-mm-ss") + ".log");
    if (logFile != nullptr) {
        logFile->open(QFile::Append | QFile::Text);
    }

    Log.setDevice(logFile);

    preInit();

    QString log = QString("ProcessOperation construction is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);
}

ProcessOperation::~ProcessOperation() {
    destroy();

    QString log = QString("~ProcessOperation destruction is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    Log.flush();

    if (logFile) {
        if (logFile->isOpen()) {
            logFile->close();
        }

        delete logFile;
        logFile = nullptr;
    }
}

bool ProcessOperation::preInit() {
    QString log = QString("preInit function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    for (auto it = this->database.databases.begin() ; it != this->database.databases.end() ; ++it) {
        QSqlDatabase *db_temp = new QSqlDatabase(QSqlDatabase::addDatabase("QMYSQL", it.value()));
        db_temp->setHostName(this->database.host_name);
        db_temp->setUserName(this->database.user_name);
        db_temp->setPassword(this->database.password);
        db_temp->setDatabaseName(it.value());

        if (!db_temp->open()) {
            log = db_temp->lastError().text() + QString(".\n database[%1] open Failed").arg(it.value());
            logToFile(&log);
            qDebug() << log;
        }

        this->dbs[it.value()] = db_temp;
    }

    return true;
}

bool ProcessOperation::destroy() {
    QString log = QString("destroy function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    for (auto it = this->dbs.begin() ; it != this->dbs.end() ; ++it) {
        QSqlDatabase *db = it.value();
        if (db->isOpen()) {
            db->commit();
            db->close();
            log = QString("Connection [%1] is Closed.").arg(it.key());
            logToFile(&log);
            qDebug() << log;
        }
        delete db;
    }
    this->dbs.clear();

    return true;
}

extern QMutex LogMutex;

void ProcessOperation::logToFile(const QString *logInfo) {
    if (logInfo == nullptr || logFile == nullptr) {
        return;
    }

    if (!logFile->isWritable()) {
        return;
    }

    LogMutex.lock();
    Log << lineNumber << ": " << *logInfo << endl;
    ++ lineNumber;
    Log.flush();
    LogMutex.unlock();
}

void ProcessOperation::jsonStringToDropList(const QString *jsonString, QList<int> &dropList) {
    dropList.clear();

    QJsonDocument json_document = QJsonDocument::fromJson(jsonString->toUtf8());
    if (!json_document.isObject()) {
        return;
    }

    QJsonObject json_object = json_document.object();
    if (json_object.find("dropped") == json_object.end()) {
        QString log = QString("Cannot find object 'dropped', finishing.[%1][%2]").arg(__func__).arg(__LINE__);
        logToFile(&log);
    }

    QJsonArray json_array = json_object["dropped"].toArray();
    for (auto item : json_array) {
        dropList.push_back(item.toInt());
    }

    std::sort(dropList.begin(), dropList.end());
}

void connectDatabase(QString connection_name, QString database_name, database_info_t *db_info) {
    if (!QSqlDatabase::contains(connection_name)) {
        QSqlDatabase db_modulation = QSqlDatabase::addDatabase("QMYSQL", connection_name);
        db_modulation.setHostName(db_info->host_name);
        db_modulation.setUserName(db_info->user_name);
        db_modulation.setPassword(db_info->password);
        db_modulation.setDatabaseName(database_name);
    }
}
