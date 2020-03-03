#ifndef PROCESSOPERATION_H
#define PROCESSOPERATION_H

#include "../include/parameters.h"

#include <QObject>
#include <QFile>
#include <QMutex>
#include <QTextStream>
#include <QSqlDatabase>
#include <QList>
#include <QSqlQuery>
#include <QRunnable>

class ProcessOperation : public QObject
{
    Q_OBJECT
public:
    explicit ProcessOperation(QObject *parent = nullptr, database_info_t *database_info = nullptr,
                              parameter_t *parameters = nullptr);
    ~ProcessOperation();

    void process();

    static void logToFile(const QString *logInfo);
    static void jsonStringToDropList(const QString *jsonString, QList<int> &dropList);

private:
    bool preInit();
    bool init();

    void processAction(QList<QString> *table);

    bool preDestroy();
    bool destroy();

    database_info_t database;
    parameter_t parameter;

    static QFile *logFile;
    static QTextStream Log;
    static volatile int lineNumber;
    static QMutex LogMutex;

    QList<QString> followTables;

    QMap<QString, QSqlDatabase*> dbs;
};


class Task : public QRunnable {
public:
    Task(database_info_t *database_info, parameter_t *parameter, const QString table_name);
    ~Task() Q_DECL_OVERRIDE;

    void run() Q_DECL_OVERRIDE;

private:
    QString table;
    parameter_t *parameter;
    database_info_t *database;

    bool preProcessDB();
};


void connectDatabase(QString connection_name, QString database_name, database_info_t *db_info);

#define logQueryError(sql_query) \
    do{\
        log = sql_query->lastError().text() + ". " + sql_query->lastQuery(); \
        qDebug() << log; \
        ProcessOperation::logToFile(&log);\
    } while(0)

#endif // PROCESSOPERATION_H
