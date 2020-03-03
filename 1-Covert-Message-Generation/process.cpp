#include "../include/processoperation.h"

#include <QDebug>
#include <QSqlError>
#include <QSqlQuery>
#include <QRandomGenerator>
#include <QDateTime>
#include <QThread>

bool ProcessOperation::init() {
    QString log = QString("init function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    QSqlDatabase *db = this->dbs.value(this->parameter["database"]);
    if (!db->isOpen() && !db->open()) {
        log = QString("Cannot open MySQL database in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
        logToFile(&log);
        qDebug() << log;
        log = db->lastError().text();
        qDebug() << log;
        logToFile(&log);

        return false;
    }

    QSqlQuery *sql = new QSqlQuery(*db);

    log = QString("MySQL database SQl is opened in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);
    qDebug() << log;

    QString sql_cmd = QString("DELETE FROM `%1`.`%2`;").arg(this->parameter["database"]).arg(this->parameter["tablename"]);
    if (!sql->exec(sql_cmd)) {
        log = sql->lastError().text() + ". " + sql->lastQuery();
        logToFile(&log);
        qDebug() << log;
        delete sql;
        return false;
    }

    sql_cmd = QString("ALTER TABLE `%1`.`%2` AUTO_INCREMENT = 1;").arg(this->parameter["database"]).arg(this->parameter["tablename"]);
    if (!sql->exec(sql_cmd)) {
        log = sql->lastError().text() + ". " + sql->lastQuery();
        logToFile(&log);
        qDebug() << log;
        delete sql;
        return false;
    }

    log = "Table is Cleared and the Index is set to 1.";
    logToFile((&log));
    delete  sql;
    return true;
}

void ProcessOperation::processAction(QList<QString> *tables) {
    QString log = QString("processAction function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    QSqlDatabase *db = this->dbs.value(this->parameter["database"]);
    QSqlQuery *sql = new QSqlQuery(*db);

    for (int i = 0 ; i < this->parameter["count"].toInt() ; ++i) {
        QString data = "";
        QRandomGenerator gen(static_cast<quint32>(QDateTime::currentDateTime().currentMSecsSinceEpoch()));
        int j = 0;
        while (j < this->parameter["length"].toInt()) {
            quint32 number = gen.generate();
            QString tempString = QString::number(number, 2);
            int remain = j + tempString.length() < this->parameter["length"].toInt() ?
                        tempString.length() : this->parameter["length"].toInt() - j;
            tempString.remove(remain, tempString.length() - remain);
            data += tempString;
            j += remain;
        }

        QString sql_cmd = QString("INSERT INTO `%1`.`%2`(`LENGTH`, `DATA`) VALUES('%3', '%4');").arg(
                    this->parameter["database"]).arg(this->parameter["tablename"]).arg(
                    this->parameter["length"].toInt()).arg(data);
        if(sql->exec(sql_cmd)) {
            log = QString("data %1 is inserted with length %2.").arg(i).arg(data.length());
            logToFile(&log);
        } else {
            log = sql->lastError().text() + ". " + sql->lastQuery();
            logToFile(&log);
            qDebug() << log;
        }
        QThread::sleep(1);
    }
    delete sql;
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
