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

bool ProcessOperation::init() {
    QString log = QString("init function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    QSqlDatabase *db = this->dbs[this->parameter["database"]];
    if (!db->isOpen() && !db->open()) {
        log = QString("Cannot open MySQL database in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
        logToFile(&log);
        qDebug() << log;
        log = db->lastError().text();
        qDebug() << log;
        logToFile(&log);

        return false;
    }


    log = QString("MySQL database SQl is opened in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);
    qDebug() << log;

    return true;
}

void ProcessOperation::processAction(QList<QString> *tables) {
    QString log = QString("processAction function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    //@TODO

    QList<double> packetDropRate;
    QStringList rateStrs = this->parameter["RATE"].split(',');
    for (auto item : rateStrs) {
        double temp = item.toDouble();
        packetDropRate.push_back(temp);
    }

    int randFragment = 1000;
    int randBegin = 1;

    QSqlDatabase *db = this->dbs[this->parameter["database"]];
    QSqlQuery *sql = new QSqlQuery(*db);

    for (auto rate : packetDropRate) {
        int dropPerThousand = static_cast<int>(static_cast<double>(randFragment) * rate);
        int roundThousand = this->parameter["LENGTH"].toInt() / randFragment;

        log = QString("Under Rate[%1], drop per thousand[%2], rounds[%3]").arg(rate).arg(dropPerThousand).arg(roundThousand);
        qDebug() << log;
        logToFile(&log);

        for (int i = 0 ; i < this->parameter["REPEAT"].toInt() ; ++i) {
            QList<int> result_list;

            qsrand(static_cast<quint32>(QDateTime::currentDateTime().currentMSecsSinceEpoch()));
            for (int j = 0 ; j < roundThousand ; ++j) {
                QSet<int> deduplicate;
                while(deduplicate.size() < dropPerThousand) {
                    int item = randBegin + qrand() % (randFragment - randBegin) + j * randFragment;
                    if (deduplicate.find(item) == deduplicate.end()) {
                        deduplicate.insert(item);
                        result_list.push_back(item);
                    }
                }

                deduplicate.clear();
            }

            std::sort(result_list.begin(), result_list.end());

            QString info = QString("Random with rate[%1], Length[%2]").arg(rate).arg(roundThousand * randFragment);
            QString drop = QString("{\"dropped\": [");
            for (auto item : result_list) {
                drop += QString::number(item) + ",";
            }
            drop = drop.remove(drop.length() - 1, 1);
            drop += "]}";
            result_list.clear();

            QString sql_cmd = QString("INSERT INTO `%1`.`%2`(`TYPE`, `MAXSEQ`, `INFO`, `SSRC`, `LOSTLIST`) "
                                  "VALUES('%3', '%4', '%5', '%6', '%7')").arg(
                        this->parameter["database"]).arg(this->parameter["tablename"]).arg(
                        this->parameter["TYPE"]).arg(roundThousand * randFragment).arg(info).arg(
                        1000000000 + qrand() % (2147483647 - 1000000000)).arg(drop);

            if (!sql->exec(sql_cmd)) {
                logQueryError(sql);
            }
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
