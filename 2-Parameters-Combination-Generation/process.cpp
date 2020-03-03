#include "../include/processoperation.h"

#include <QDebug>
#include <QSqlError>
#include <QSqlQuery>
#include <QRandomGenerator>
#include <QDateTime>
#include <QThread>
#include <QMap>
#include <QList>
#include <QStringList>
#include <QStack>

bool ProcessOperation::init() {
    QString log = QString("init function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    QSqlDatabase *db_source = this->dbs[this->parameter["database"]];
    if (!db_source->isOpen() && !db_source->open()) {
        log = QString("Cannot open MySQL database in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
        logToFile(&log);
        qDebug() << log;
        log = db_source->lastError().text();
        qDebug() << log;
        logToFile(&log);

        return false;
    }

    QSqlQuery sql = QSqlQuery(*db_source);

    log = QString("MySQL database SQl is opened in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);
    qDebug() << log;

    QString sql_cmd = QString("DELETE FROM `%1`.`%2`;").arg(this->parameter["database"]).arg(this->parameter["tablename"]);
    if (!sql.exec(sql_cmd)) {
        log = sql.lastError().text() + ". " + sql.lastQuery();
        logToFile(&log);
        qDebug() << log;
        return false;
    }

    sql_cmd = QString("ALTER TABLE `%1`.`%2` AUTO_INCREMENT = 1;").arg(this->parameter["database"]).arg(this->parameter["tablename"]);
    if (!sql.exec(sql_cmd)) {
        log = sql.lastError().text() + ". " + sql.lastQuery();
        logToFile(&log);
        qDebug() << log;
        return false;
    }

    sql.clear();
    sql.finish();

    log = "Table is Cleared and the Index is set to 1.";
    logToFile((&log));
    return true;
}

void ProcessOperation::processAction(QList<QString> *tables) {
    QString log = QString("processAction function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    //@TODO

    QMap< QString, QList<unsigned int> > args;

    QList <QString> baseArgs = {"LCODEWORD", "R"};
    QList <QString> suppleArgs = {"LHASH", "LCRC", "MCOLS"};

    for (auto key : baseArgs) {
        QList<unsigned int> temp;
        QStringList strList = this->parameter[key].split(',');
        for (auto item : strList) {
            temp.push_back(item.toUInt());
        }
        args[key] = temp;
    }

    if (this->parameter["MODE"].toInt() == 2) {
        for (auto key : suppleArgs) {
            QList<unsigned int> temp;
            QStringList strList = this->parameter[key].split(',');
            for (auto item : strList) {
                temp.push_back(item.toUInt());
            }
            args[key] = temp;
        }
    }

    QList< QList<unsigned int> > argCombinations;
    QList<QString> keys = args.keys();
    for (auto key : keys) {
        QList< QList<unsigned int> > tempResult;
        QList<unsigned int> values = args[key];

        if (argCombinations.size() == 0) {
            for (auto val : values) {
                QList<unsigned int> temp;
                temp.push_back(val);
                argCombinations.push_back(temp);
            }
        } else {
            for (auto origin : argCombinations) {
                for (auto append : values) {
                    QList <unsigned int> temp = origin;
                    temp.push_back(append);
                    tempResult.push_back(temp);
                }
            }
            argCombinations.clear();
            argCombinations = tempResult;
        }
    }

    log = QString("generated combinations count = %1").arg(argCombinations.size());
    logToFile(&log);
    qDebug() << log;

    if (this->parameter["MODE"].toInt() == 2) {
        QList< QList<unsigned int> > tempResult;

        int indexOfLcodeword = keys.indexOf("LCODEWORD");
        int indexOfLcrc = keys.indexOf("LCRC");
        int indexOfLhash = keys.indexOf("LHASH");

        for (auto item : argCombinations) {
            if (item[indexOfLcrc] + item[indexOfLhash] < item[indexOfLcodeword]) {
                tempResult.push_back(item);
            }
        }

        argCombinations.clear();
        argCombinations = tempResult;

        log = QString("Fixed combinations count = %1").arg(argCombinations.size());
        logToFile(&log);
        qDebug() << log;
    }

    QString sql_template = QString("INSERT INTO `%1`.`%2`(").arg(
                this->parameter["database"]).arg(this->parameter["tablename"]);
    for (auto key : keys) {
        sql_template += "`" + key + "`,";
    }
    sql_template.remove(sql_template.length() - 1, 1);
    sql_template += ") VALUES (";

    QSqlQuery sql = QSqlQuery(*(this->dbs[this->parameter["database"]]));
    for (auto item : argCombinations) {
        QString sql_cmd = "";
        if (this->parameter["MODE"].toInt() == 2) {
            QString temp = QString("'%1', '%2', '%3', '%4', '%5');").arg(
                        item[0]).arg(item[1]).arg(item[2]).arg(item[3]).arg(item[4]);
            sql_cmd = sql_template + temp;
        } else {
            QString temp = QString("'%1', '%2');").arg(item[0]).arg(item[1]);
            sql_cmd = sql_template + temp;
        }

        if (!sql.exec(sql_cmd)) {
            log = sql.lastError().text() + ". " + sql.lastQuery();
            logToFile(&log);
            qDebug() << log;
        }
    }
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

    processAction(nullptr);

    preDestroy();
}
