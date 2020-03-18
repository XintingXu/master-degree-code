#include "../include/processoperation.h"

#include <QDebug>
#include <QSqlError>
#include <QSqlQuery>
#include <QDateTime>
#include <QThread>
#include <QMap>
#include <QList>
#include <QStringList>
#include <QStack>
#include <QSet>
#include <QThreadPool>
#include <QStringRef>
#include <QCryptographicHash>
#include <QPoint>
#include <QHash>

static QList<int> g_captureIDs;
static QList<int> g_dataIDs;
static QMap<int, QString> g_dataMap;
static QMap<int, int> g_captureLengthMap;
static QList<int> g_dropWindow;

bool ProcessOperation::init() {
    QString log = QString("init function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    QSqlDatabase *db_modulation = this->dbs[this->parameter["database_modulation"]];
    if (db_modulation->isOpen()) {
        QSqlQuery *sql_source = new QSqlQuery(*db_modulation);

        QString sql_cmd = QString("SELECT CONCAT(\"DROP TABLE IF EXISTS `%1`.`\", table_name, \"`;\") "
                                  "from information_schema.tables where table_schema=\"%1\";").arg(
                                  this->database.databases["modulation"]);
        logToFile(&sql_cmd);

        if (!sql_source->exec(sql_cmd)) {
            log = sql_source->lastError().text() + ". " + sql_source->lastQuery();
            qDebug() << log;
            logToFile(&log);
            sql_source->clear();
            delete sql_source;
            return false;
        }

        sql_cmd = "";
        while (sql_source->next()) {
            sql_cmd += sql_source->value(0).toString();
        }
        if (sql_cmd.length()) {
            logToFile(&sql_cmd);
            if (!sql_source->exec(sql_cmd)) {
                logQueryError(sql_source);
            }
        }

        sql_source->clear();
        delete sql_source;
        db_modulation->commit();
    } else {
        log = db_modulation->lastError().text();
        qDebug() << log;
        logToFile(&log);
        return false;
    }

    return true;
}


Task::Task(database_info_t *database_info, parameter_t *parameter, const QString table_name) {
    this->table = table_name;
    this->parameter = parameter;
    this->database = database_info;
}

Task::~Task() {

}

bool Task::preProcessDB() {
    QString con_source = this->database->databases["source"] + "_" +
            QString::number(uintptr_t(QThread::currentThreadId()));
    connectDatabase(con_source, this->database->databases["source"], this->database);
    QSqlDatabase db_source = QSqlDatabase::database(con_source);

    QString con_modulation = this->database->databases["modulation"] + "_" +
            QString::number(uintptr_t(QThread::currentThreadId()));
    connectDatabase(con_modulation, this->database->databases["modulation"], this->database);
    QSqlDatabase db_modulation = QSqlDatabase::database(con_modulation);

    QString log;
    if (!db_source.isOpen() && !db_source.open()) {
        log = QString("cannot open db [%1] for thread [%2], exiting").
                arg((*this->parameter)["database_source"]).arg(this->table);
        ProcessOperation::logToFile(&log);

        return false;
    }

    if (!db_modulation.isOpen() && !db_modulation.open()) {
        log = QString("cannot open db [%1] for thread [%2], exiting").
                arg((*this->parameter)["database_modulation"]).arg(this->table);
        ProcessOperation::logToFile(&log);

        db_source.close();
        return false;
    }

    QSqlQuery *sql_source = new QSqlQuery(db_source);
    QSqlQuery *sql_modulation = new QSqlQuery(db_modulation);

    QString sql_cmd = QString("CREATE TABLE IF NOT EXISTS `%1`.`%2` "
                              "( `ID` INT NOT NULL AUTO_INCREMENT COMMENT '数据索引' , "
                              "`PARAID` INT NOT NULL COMMENT '参数配置的ID' , "
                              "`SOURCEID` INT NOT NULL COMMENT '噪声数据的ID序号' , "
                              "`DATAID` INT NOT NULL COMMENT '发送数据的ID序号' , "
                              "`DROPLIST` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '调制后的丢包序列' , "
                              "`DEMODULATED` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '解调得到的二进制序列' , "
                              "`SENTBITS` INT UNSIGNED NOT NULL DEFAULT '0' COMMENT '发送位数' , "
                              "`ERRORBITS` INT UNSIGNED NOT NULL DEFAULT '0' COMMENT '错误位数' , "
                              "`UPDATETIME` TIMESTAMP on update CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据更新时间' , "
                              "`CREATETIME` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间' , "
                              "PRIMARY KEY (`ID`), "
                              "INDEX `data` (`DATAID`), "
                              "INDEX `id` (`ID`), "
                              "INDEX `source` (`SOURCEID`), "
                              "INDEX `para` (`PARAID`)"
                              ") ENGINE = InnoDB "
                              "CHARSET=utf8mb4 "
                              "COLLATE utf8mb4_unicode_ci "
                              "COMMENT = '调制解调详情表，%1';").arg((*this->parameter)["database_modulation"]).arg(this->table);

    if (!sql_modulation->exec(sql_cmd)) {
        log = QString("Cannot create table `%1`\n").arg(this->table) +
                sql_modulation->lastError().text() + sql_modulation->lastQuery();
        ProcessOperation::logToFile(&log);
    }

    sql_cmd = QString("DELETE FROM `%1`.`%2`;").arg((*this->parameter)["database_modulation"]).arg(this->table);
    if (!sql_modulation->exec(sql_cmd)) {
        log = QString("Cannot clear table `%1`\n").arg(this->table) +
                sql_modulation->lastError().text() + sql_modulation->lastQuery();
        ProcessOperation::logToFile(&log);
    }

    sql_source->clear();
    sql_source->finish();
    sql_modulation->clear();
    sql_modulation->finish();

    delete sql_source;
    delete sql_modulation;

    db_modulation.commit();

    return true;
}

QString modulation(int sourceID, int window) {
    QString result;

    int max_sequence_number = g_captureLengthMap[sourceID];
    int window_count = max_sequence_number / window;

    QList<int> dropped_list;
    qsrand(QTime::currentTime().msecsSinceStartOfDay());

    for (int count = 0 ; count < window_count ; ++ count) {
        int offset = qrand() % (window) + 1;
        int seq = window * count + offset;
        dropped_list.push_back(seq);
    }

    std::sort(dropped_list.begin(), dropped_list.end());
    praseIntListtoCSV(&dropped_list, result);

    return result;
}

void Task::run() {
    QString log = QString("Thread is actived, table name :%1").arg(this->table);
    ProcessOperation::logToFile(&log);

    if (!preProcessDB()) {
        log = QString("Preprocess for Database is failed. table [%1]").arg(this->table);
        ProcessOperation::logToFile(&log);
        return;
    }

    QString con_modulation = this->database->databases["modulation"] + "_" +
            QString::number(uintptr_t(QThread::currentThreadId()));

    QSqlQuery *sql_modulation = new QSqlQuery(QSqlDatabase::database(con_modulation));

        for (auto capture : g_captureIDs) {
            for (auto data : g_dataIDs) {
                QString drop_list = modulation(capture, this->table.toInt());

                QString sql_cmd = QString("INSERT INTO `%1`.`%2`(`PARAID`,`SOURCEID`,`DATAID`,`DROPLIST`,`SENTBITS`) "
                                  "VALUES('%3','%4','%5','%6','%7')").arg(this->database->databases["modulation"]
                                  ).arg(this->table).arg(this->table.toInt()).arg(capture).arg(data).arg(drop_list).arg(0);

                if (!sql_modulation->exec(sql_cmd)) {
                    logQueryError(sql_modulation);
                }
            }
        }

    sql_modulation->clear();
    sql_modulation->finish();

    delete sql_modulation;

    log = QString("Thread for [%1] is finished.").arg(this->table);
    ProcessOperation::logToFile(&log);
    qDebug() << log;
}

void ProcessOperation::processAction(QList<QString> *tables) {
    QString log = QString("processAction function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    //@TODO
    QThreadPool pool(this);
    pool.setMaxThreadCount(this->parameter["THREADS"].toInt());

    for (auto table : *tables) {
        Task *task = new Task(&this->database, &this->parameter, table);
        pool.start(task);
        log = QString("Thread for %1 is actived.").arg(table);
        logToFile(&log);
    }

    pool.waitForDone();
}

bool ProcessOperation::preDestroy() {
    QString log = QString("preDestroy function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    return true;
}

void ProcessOperation::process() {
    QString log = QString("process function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    if (!init()) {
        return;
    }

    QSqlDatabase *db_source = this->dbs[parameter["database_source"]];
    if (!db_source->isOpen() && !db_source->open()) {
        log = QString("Cannot open MySQL database in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
        logToFile(&log);
        qDebug() << log;
        log = db_source->lastError().text();
        qDebug() << log;
        logToFile(&log);
        return;
    } else {
        log = QString("MySQL database SQl is opened in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
        logToFile(&log);
        qDebug() << log;
    }
    QSqlQuery *sql_source = new QSqlQuery(*db_source);

    QList<QString> targetTables;

    QStringList win_strs = this->parameter["WINDOW"].split(',');
    for (auto item : win_strs) {
        g_dropWindow.push_back(QString(item).toInt());
    }

    for (auto win : g_dropWindow) {
        targetTables.push_back(QString("%1").arg(win));
    }

    log = QString("target Table length = %1").arg(targetTables.size());
    qDebug() << log;
    logToFile(&log);

    QList<int> types;
    QString typesCondition;
    QStringList typeList = QString(this->parameter["TYPE"]).split(',');
    for (auto type : typeList) {
        types.push_back(type.toInt());
        typesCondition += "`TYPE`='" + type + "'";
        if (typeList.indexOf(type) != typeList.length() - 1) {
            typesCondition += " OR ";
        }
    }
    QString sql_cmd = QString("SELECT `ID`,`MAXSEQ` FROM `%1`.`%2` WHERE %3").arg(this->database.databases["source"]).arg(
                this->parameter["tablename_captured"]).arg(typesCondition);
    g_captureIDs.clear();
    g_captureLengthMap.clear();
    if (sql_source->exec(sql_cmd)) {
        while (sql_source->next()) {
            int id = sql_source->value("ID").toInt();
            g_captureIDs.push_back(id);
            g_captureLengthMap[id] = sql_source->value("MAXSEQ").toInt();
        }
    } else {
        logQueryError(sql_source);
    }
    sql_source->clear();
    sql_cmd = QString("SELECT `ID` FROM `%1`.`%2`").arg(this->database.databases["source"]).arg(
                this->parameter["tablename_data"]);
    g_dataIDs.clear();
    if (sql_source->exec(sql_cmd)) {
        while (sql_source->next()) {
            int id = sql_source->value("ID").toInt();
            g_dataIDs.push_back(id);
        }
    } else {
        logQueryError(sql_source);
    }
    sql_source->clear();

    db_source->close();
    delete sql_source;

    processAction(&targetTables);

    preDestroy();
}
