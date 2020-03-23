#include "../include/processoperation.h"
#include "../include/parameters.h"

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
static QMap<int, QMap<int, int> > g_captureIDTimeMap;
static QMap<int, QList<int> > g_captureIDDropList;
static QMap<int, int> g_capturedIDMAXSeq;

bool ProcessOperation::init() {
    QString log = QString("init function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    QSqlDatabase *db_distribution = this->dbs[this->parameter["database_distribution"]];
    if (db_distribution->open()) {
        QSqlQuery *sql_distribution = new QSqlQuery(*db_distribution);

        QString sql_cmd = QString("SELECT CONCAT(\"DROP TABLE IF EXISTS `%1`.`\", table_name, \"`;\") "
                                  "from information_schema.tables where table_schema=\"%1\";").arg(
                                  this->database.databases["distribution"]);
        logToFile(&sql_cmd);

        if (!sql_distribution->exec(sql_cmd)) {
            logQueryError(sql_distribution);
            sql_distribution->clear();
            delete sql_distribution;
            return false;
        }

        sql_cmd = "";
        while (sql_distribution->next()) {
            sql_cmd += sql_distribution->value(0).toString();
        }
        if (sql_cmd.length()) {
            logToFile(&sql_cmd);
            if (!sql_distribution->exec(sql_cmd)) {
                logQueryError(sql_distribution);
            }
        }

        sql_distribution->clear();
        delete sql_distribution;
        db_distribution->commit();
    } else {
        log = db_distribution->lastError().text();
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

    QString con_distribution = this->database->databases["distribution"] + "_" +
            QString::number(uintptr_t(QThread::currentThreadId()));
    connectDatabase(con_distribution, this->database->databases["distribution"], this->database);
    QSqlDatabase db_distribution = QSqlDatabase::database(con_distribution);

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

    if (!db_distribution.isOpen() && !db_distribution.open()) {
        log = QString("cannot open db [%1] for thread [%2], exiting").
                arg((*this->parameter)["database_distribution"]).arg(this->table);
        ProcessOperation::logToFile(&log);

        db_source.close();
        db_modulation.close();
        return false;
    }

    return true;
}

void csvStringToList(QString *source, QList<int> &result) {
    QStringList items = source->split(',');
    result.clear();
    for (auto item : items) {
        result.push_back(item.toInt());
    }
}

void appendIPDs(QList<int> *dropList, QMap<int, int> *reference, QMap<int, int> &result) {
    QList<int> referenceKeys = reference->keys();
    std::sort(referenceKeys.begin(), referenceKeys.end());
    int MAX_SEQUENCE = referenceKeys.last();

    QMap<int, int> remainingPackets;
    for (int seq = 1 ; seq <= MAX_SEQUENCE ; ++seq) {
        if (dropList->indexOf(seq) < 0) {   // not dropped
            remainingPackets[seq] = reference->value(seq);
        }
    }

    int lastTimeStamp = 0;
    for (auto it = remainingPackets.begin() ; it != remainingPackets.end() ; ++it) {
        int ipd = it.value() - lastTimeStamp;
        Q_ASSERT(ipd >= 0);
        if (result.find(ipd) == result.end()) {
            result[ipd] = 0;
        }
        result[ipd] = result[ipd] + 1;
        lastTimeStamp = it.value();
    }
}

void appendWinPacketLoss(QList<int> *dropList, QMap<int, int> &result, int max_length, int win_size) {
    if (result.size() < win_size + 1) {
        for (int i = 0 ; i <= win_size ; ++i) {
            if (result.find(i) == result.end()) {
                result[i] = 0;
            }
        }
    }

    int beginIndex = 1;
    int index = 0;
    while (beginIndex < max_length) {
        int count = 0;
        while (index < dropList->size()) {
            if (dropList->at(index) >= beginIndex && dropList->at(index) < beginIndex + win_size) {
                ++ index;
                ++ count;
            } else {
                break;
            }
        }

        beginIndex += win_size;
        if (result.find(count) == result.end()) {
            result[count] = 0;
        }
        result[count] = result[count] + 1;
    }
}


// Continuous packet dropout count
void appendCountOfPacketDropout(QList<int> *dropList, QMap<int, int> &result) {
    int indexMax = dropList->size();
    int index = 1;
    int count = 1;

    while(index < indexMax) {
        if (dropList->at(index - 1) + 1 == dropList->at(index)) {
            ++ count;
        } else {
            if (result.find(count) == result.end()) {
                result[count] = 1;
            } else {
                result[count] += 1;
            }

            count = 1;
        }

        ++ index;
    }
}

// Change map result into CDF map
void changeToCDF(QMap<int, int> *source, QMap<int, double> &result) {
    QList<int> keys = source->keys();
    std::sort(keys.begin(), keys.end());

    double sum = 0;
    for (auto it = source->begin() ; it != source->end() ; ++it) {
        sum += double(it.value());
    }

    if (sum < 0.1) {
        return;
    }

    double current = 0.0;
    for (auto key : keys) {
        current += source->value(key);
        result[key] = current / sum;
    }
}

// change map into pmf function
void changeToPMF(QMap<int, int> *source, QMap<int, double> &result) {
    double sum = 0.0;
    for (auto it = source->begin() ; it != source->end() ; ++it) {
        sum += double(it.value());
    }
    for (auto it = source->begin() ; it != source->end() ; ++it) {
        result[it.key()] = double(it.value()) / sum;
    }
}


// generate sql col_name and col_values
template<typename T>
void generateColPair(parameter_t *parameters, QMap<int, QMap<int, T> > *source, QString type, QString &col_name, QString &col_value) {
    col_name.clear();
    col_value.clear();

    for (int i = parameters->value("WIN_BEGIN").toInt();
         i < parameters->value("WIN_END").toInt();
         i += parameters->value("WIN_STEP").toInt()) {
        col_name += QString("`WIN%1%2`,").arg(i).arg(type);
        col_value += QString("'%1',").arg(praseMapToJSON(&(*source)[i]));
    }
    col_name += QString("`WIN%1%2`").arg(parameters->value("WIN_END").toInt()).arg(type);
    col_value += QString("'%1'").arg(praseMapToJSON(&(*source)[parameters->value("WIN_END").toInt()]));
}


void Task::run() {
    QString log = QString("Thread is actived, table name :%1").arg(this->table);
    ProcessOperation::logToFile(&log);
    qDebug() << log;

    if (!preProcessDB()) {
        log = QString("Preprocess for Database is failed. table [%1]").arg(this->table);
        return;
    }

    QString con_source = this->database->databases["source"] + "_" +
            QString::number(uintptr_t(QThread::currentThreadId()));
    QString con_modulation = this->database->databases["modulation"] + "_" +
            QString::number(uintptr_t(QThread::currentThreadId()));
    QString con_distribution = this->database->databases["distribution"] + "_" +
            QString::number(uintptr_t(QThread::currentThreadId()));

    QSqlQuery *sql_source = new QSqlQuery(QSqlDatabase::database(con_source));
    QSqlQuery *sql_modulation = new QSqlQuery(QSqlDatabase::database(con_modulation));
    QSqlQuery *sql_distribution = new QSqlQuery(QSqlDatabase::database(con_distribution));

    for (auto id : g_captureIDs) {
        QString sql_cmd = QString("SELECT `DROPLIST` FROM `%1`.`%2` WHERE `SOURCEID`='%3';").arg(
                                  this->database->databases["modulation"]).arg(this->table).arg(id);
        if (!sql_modulation->exec(sql_cmd)) {
            logQueryError(sql_modulation);
        }

        QMap<int, int> IPDS;
        QMap<int, int> COUNT;
        QMap<int, QMap<int, int> > WIN;
        for (int i = this->parameter->value("WIN_BEGIN").toInt() ;
             i <= this->parameter->value("WIN_END").toInt() ;
             i += this->parameter->value("WIN_STEP").toInt()) {
            QMap<int, int> temp;
            WIN[i] = temp;
        }

        QList<QString> dropLists;
        while (sql_modulation->next()) {
            dropLists.push_back(sql_modulation->value("DROPLIST").toString());
        }

        for (auto item : dropLists) {
            QList<int> dropped;
            csvStringToList(&item, dropped);
            if (g_captureIDDropList.find(id) != g_captureIDDropList.end()) {
                dropped += g_captureIDDropList[id];
                QSet<int> uniqueDropped = QSet<int>::fromList(dropped);
                if (uniqueDropped.contains(0)) {
                    uniqueDropped.remove(0);
                }
                dropped = QList<int>::fromSet(uniqueDropped);
                std::sort(dropped.begin(), dropped.end());
            }

            appendIPDs(&dropped, &g_captureIDTimeMap[id], IPDS);
            appendCountOfPacketDropout(&dropped, COUNT);

            for (auto key : WIN.keys()) {
                appendWinPacketLoss(&dropped, WIN[key], g_capturedIDMAXSeq[id], key);
            }
        }

        QString col_name;
        QString col_value;
        generateColPair(this->parameter, &WIN, "DIS", col_name, col_value);
        sql_cmd = QString("INSERT INTO `%1`.`distribution_%2` "
                          "(`SOURCEID`,`PARAID`,`IPDDIS`,`COUNTDIS`,%6) "
                          "VALUES ('%2','%3','%4','%5',%7)").arg(
                          this->database->databases["distribution"]).arg(id).arg(
                          this->table).arg(praseMapToJSON(&IPDS)).arg(praseMapToJSON(&COUNT)).arg(
                          col_name).arg(col_value);

        if (!sql_distribution->exec(sql_cmd)) {
            logQueryError(sql_distribution);
        }

        //@TODO
        /*CDF result, full reference distribution consistency test*/
        QMap<int, double> cdfIPD;
        QMap<int, double> cdfCOUNT;
        QMap<int, QMap<int, double> > cdfWIN;

        changeToCDF(&IPDS, cdfIPD);
        changeToCDF(&COUNT, cdfCOUNT);
        for (auto key : WIN.keys()) {
            QMap<int, double> temp;
            changeToCDF(&WIN[key], temp);
            cdfWIN[key] = temp;
        }

        generateColPair(this->parameter, &cdfWIN, "CDF", col_name, col_value);
        sql_cmd = QString("INSERT INTO `%1`.`cdf_%2` "
                          "(`SOURCEID`,`PARAID`,`IPDCDF`,`COUNTCDF`,%6) "
                          "VALUES ('%2','%3','%4','%5',%7)").arg(
                          this->database->databases["distribution"]).arg(id).arg(
                          this->table).arg(praseMapToJSON(&cdfIPD)).arg(praseMapToJSON(&cdfCOUNT)).arg(
                          col_name).arg(col_value);

        if (!sql_distribution->exec(sql_cmd)) {
            logQueryError(sql_distribution);
        }

        /*PMF result, for KL-D and others*/
        QMap<int, double> pmfIPD;
        QMap<int, double> pmfCOUNT;
        QMap<int, QMap<int, double> > pmfWIN;

        changeToPMF(&IPDS, pmfIPD);
        changeToPMF(&COUNT, pmfCOUNT);
        for (auto key : WIN.keys()) {
            QMap<int, double> temp;
            changeToPMF(&WIN[key], temp);
            pmfWIN[key] = temp;
        }

        generateColPair(this->parameter, &pmfWIN, "PMF", col_name, col_value);
        sql_cmd = QString("INSERT INTO `%1`.`pmf_%2` "
                          "(`SOURCEID`,`PARAID`,`IPDPMF`,`COUNTPMF`,%6) "
                          "VALUES ('%2','%3','%4','%5',%7)").arg(
                          this->database->databases["distribution"]).arg(id).arg(
                          this->table).arg(praseMapToJSON(&pmfIPD)).arg(praseMapToJSON(&pmfCOUNT)).arg(
                          col_name).arg(col_value);

        if (!sql_distribution->exec(sql_cmd)) {
            logQueryError(sql_distribution);
        }
    }

    sql_source->clear();
    sql_modulation->clear();
    sql_distribution->clear();
    sql_source->finish();
    sql_modulation->finish();
    sql_distribution->finish();

    delete sql_source;
    delete sql_modulation;
    delete sql_distribution;

    log = QString("table finished :%1").arg(this->table);
    ProcessOperation::logToFile(&log);
    qDebug() << log;
}

void ProcessOperation::processAction(QList<QString> *tables) {
    QString log = QString("processAction function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    QSqlDatabase *db_distribution = this->dbs[this->parameter["database_distribution"]];
    if (!db_distribution->isOpen() && !db_distribution->open()) {
        log = "Cannot open Database distribution.\n" + db_distribution->lastError().text();
        ProcessOperation::logToFile(&log);
        qDebug() << log;
    }
    QSqlQuery *sql_distribution = new QSqlQuery(*db_distribution);

    for (auto id : g_captureIDs) {
        QString sql_cmd = QString("CREATE TABLE IF NOT EXISTS `%1`.`distribution_%2` ( "
                                  "`ID` INT NOT NULL AUTO_INCREMENT COMMENT '数据索引' , "
                                  "`SOURCEID` INT NOT NULL COMMENT '抓包序列的ID' , "
                                  "`PARAID` INT NOT NULL COMMENT '参数配置的ID' , "
                                  "`IPDDIS` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'IPD分布' , "
                                  "`COUNTDIS` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '连续丢包数分布' , "
                                  ).arg(this->database.databases["distribution"]).arg(id);
        for (int i = this->parameter["WIN_BEGIN"].toInt();
             i <= this->parameter["WIN_END"].toInt() ;
             i += this->parameter["WIN_STEP"].toInt()) {
            sql_cmd += QString("`WIN%1DIS` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL "
                                "DEFAULT NULL COMMENT '%1数据包窗口丢包数量的统计分布' , ").arg(i);
        }
        sql_cmd += QString("`UPDATETIME` TIMESTAMP on update CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据更新时间' , "
                           "`CREATETIME` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间' , "
                           "PRIMARY KEY (`ID`), "
                           "INDEX `para` (`PARAID`), "
                           "INDEX `id` (`ID`), "
                           "INDEX `source` (`SOURCEID`) "
                           ") ENGINE = InnoDB "
                           "CHARSET=utf8mb4 "
                           "COLLATE utf8mb4_unicode_ci "
                           "COMMENT = '分布信息统计表';");

        QString sql_cmd2 = QString("CREATE TABLE IF NOT EXISTS `%1`.`evaluation_%2` ( "
                                   "`ID` INT NOT NULL AUTO_INCREMENT COMMENT '数据索引' , "
                                   "`SOURCEID` INT NOT NULL COMMENT '抓包序列的ID' , "
                                   "`PARAID` INT NOT NULL COMMENT '参数配置的ID' , "
                                   "`KSP-IPD` DOUBLE NOT NULL COMMENT 'IPD分布K-S测试的p值' , "
                                   "`KLD-IPD` DOUBLE NOT NULL COMMENT 'IPD分布KLD测试的熵值' , "
                                   "`TTESTP-IPD` DOUBLE NOT NULL COMMENT 'IPD分布Ttest的p值' , "
                                   "`WHITNEYP-IPD` DOUBLE NOT NULL COMMENT 'IPD分布Mann-Whitney test的p值' , "
                                   "`ANSARIP-IPD` DOUBLE NOT NULL COMMENT 'IPD分布Ansari-Bradley test的p值' , "
                                   "`WASSERSTEIN-IPD` DOUBLE NOT NULL COMMENT 'IPD分布WASSERSTEIN 距离' , "
                                   "`ENERGY-IPD` DOUBLE NOT NULL COMMENT 'IPD分布ENERGY距离' , "
                                   "`KSP-COUNT` DOUBLE NOT NULL COMMENT 'COUNT分布K-S测试的p值' , "
                                   "`KLD-COUNT` DOUBLE NOT NULL COMMENT 'COUNT分布KLD测试的熵值' , "
                                   "`TTESTP-COUNT` DOUBLE NOT NULL COMMENT 'COUNT分布Ttest的p值' , "
                                   "`WHITNEYP-COUNT` DOUBLE NOT NULL COMMENT 'COUNT分布Mann-Whitney test的p值' , "
                                   "`ANSARIP-COUNT` DOUBLE NOT NULL COMMENT 'COUNT分布Ansari-Bradley test的p值' , "
                                   "`WASSERSTEIN-COUNT` DOUBLE NOT NULL COMMENT 'COUNT分布WASSERSTEIN 距离' , "
                                   "`ENERGY-COUNT` DOUBLE NOT NULL COMMENT 'COUNT分布ENERGY距离' , ").arg(
                                   this->database.databases["distribution"]).arg(id);
        for (int i = this->parameter["WIN_BEGIN"].toInt();
             i <= this->parameter["WIN_END"].toInt() ;
             i += this->parameter["WIN_STEP"].toInt()) {
            sql_cmd2 += QString("`KLD-WIN%1` DOUBLE NOT NULL COMMENT 'WIN%1分布KLD测试的熵值' , "
                                "`WASSERSTEIN-WIN%1` DOUBLE NOT NULL COMMENT 'WIN%分布WASSERSTEIN 距离' , "
                                "`ENERGY-WIN%1` DOUBLE NOT NULL COMMENT 'WIN%分布ENERGY距离' , ").arg(i);
        }
        sql_cmd2 += QString("`UPDATETIME` TIMESTAMP on update CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录的更新时间' , "
                            "`CREATETIME` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录的创建时间' , "
                            "PRIMARY KEY (`ID`), "
                            "INDEX `id` (`ID`)"
                            ") ENGINE = InnoDB "
                            "CHARSET=utf8mb4 "
                            "COLLATE utf8mb4_unicode_ci "
                            "COMMENT = '分布统计量化评估结果表';");

        QString sql_cmd3 = QString("CREATE TABLE IF NOT EXISTS `%1`.`cdf_%2` ( "
                                   "`ID` INT NOT NULL AUTO_INCREMENT COMMENT '数据索引' , "
                                   "`SOURCEID` INT NOT NULL COMMENT '抓包序列的ID' , "
                                   "`PARAID` INT NOT NULL COMMENT '参数配置的ID' , "
                                   "`IPDCDF` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'IPD分布的累积分布函数' , "
                                   "`COUNTCDF` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '连续丢包个数分布的累积分布函数' , "
                                   ).arg(this->database.databases["distribution"]).arg(id);
        for (int i = this->parameter["WIN_BEGIN"].toInt();
             i <= this->parameter["WIN_END"].toInt() ;
             i += this->parameter["WIN_STEP"].toInt()) {
            sql_cmd3 += QString("`WIN%1CDF` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL "
                                "DEFAULT NULL COMMENT '%1数据包窗口丢包数量的累积分布函数' , ").arg(i);
        }
        sql_cmd3 += QString("`UPDATETIME` TIMESTAMP on update CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据更新时间' , "
                            "`CREATETIME` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间' , "
                            "PRIMARY KEY (`ID`), "
                            "INDEX `para` (`PARAID`), "
                            "INDEX `id` (`ID`), "
                            "INDEX `source` (`SOURCEID`) "
                            ") ENGINE = InnoDB "
                            "CHARSET=utf8mb4 "
                            "COLLATE utf8mb4_unicode_ci "
                            "COMMENT = '分布信息的累积分布函数表';");

        QString sql_cmd4 = QString("CREATE TABLE IF NOT EXISTS `%1`.`pmf_%2` ( "
                                   "`ID` INT NOT NULL AUTO_INCREMENT COMMENT '数据索引' , "
                                   "`SOURCEID` INT NOT NULL COMMENT '抓包序列的ID' , "
                                   "`PARAID` INT NOT NULL COMMENT '参数配置的ID' , "
                                   "`IPDPMF` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'IPD分布的概率密度函数' , "
                                   "`COUNTPMF` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT '连续丢包个数分布的概率密度函数' , "
                                   ).arg(this->database.databases["distribution"]).arg(id);


        for (int i = this->parameter["WIN_BEGIN"].toInt();
             i <= this->parameter["WIN_END"].toInt() ;
             i += this->parameter["WIN_STEP"].toInt()) {
            sql_cmd4 += QString("`WIN%1PMF` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL "
                                "DEFAULT NULL COMMENT '%1数据包窗口丢包数量的概率密度函数' , ").arg(i);
        }
        sql_cmd4 += QString("`UPDATETIME` TIMESTAMP on update CURRENT_TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据更新时间' , "
                            "`CREATETIME` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间' , "
                            "PRIMARY KEY (`ID`), "
                            "INDEX `para` (`PARAID`), "
                            "INDEX `id` (`ID`), "
                            "INDEX `source` (`SOURCEID`) "
                            ") ENGINE = InnoDB "
                            "CHARSET=utf8mb4 "
                            "COLLATE utf8mb4_unicode_ci "
                            "COMMENT = '分布信息的概率密度函数表';");


        if (!sql_distribution->exec(sql_cmd) ||
            !sql_distribution->exec(sql_cmd2) ||
            !sql_distribution->exec(sql_cmd3) ||
            !sql_distribution->exec(sql_cmd4)) {
            logQueryError(sql_distribution);
        }

        sql_cmd = QString("DELETE FROM `%1`.`distribution_%2`;"
                          "DELETE FROM `%1`.`evaluation_%2`;"
                          "DELETE FROM `%1`.`cdf_%2`;"
                          "DELETE FROM `%1`.`pmf_%2`;").arg(
                          this->database.databases["distribution"]).arg(id);
        if (!sql_distribution->exec(sql_cmd)) {
            logQueryError(sql_distribution);
        }

        QList<int> dropped = g_captureIDDropList[id];
        QMap<int, int> IPDS;
        QMap<int, int> COUNT;
        QMap<int, QMap<int, int> > WIN;

        appendIPDs(&dropped, &g_captureIDTimeMap[id], IPDS);
        appendCountOfPacketDropout(&dropped, COUNT);
        for (int i = this->parameter["WIN_BEGIN"].toInt();
             i <= this->parameter["WIN_END"].toInt() ;
             i += this->parameter["WIN_STEP"].toInt()) {
            QMap<int, int> temp;
            appendWinPacketLoss(&dropped, temp, g_capturedIDMAXSeq[id], i);
            WIN[i] = temp;
        }

        QString col_name;
        QString col_values;
        generateColPair(&this->parameter, &WIN, "DIS", col_name, col_values);
        sql_cmd = QString("INSERT INTO `%1`.`distribution_%2` "
                          "(`SOURCEID`,`PARAID`,`IPDDIS`,`COUNTDIS`,%5) "
                          "VALUES ('%2','0','%3','%4',%6)").arg(
                          this->parameter["database_distribution"]).arg(id).arg(
                          praseMapToJSON(&IPDS)).arg(praseMapToJSON(&COUNT)).arg(col_name).arg(col_values);
        if (!sql_distribution->exec(sql_cmd)) {
            logQueryError(sql_distribution);
        }

        QMap<int, double> cdfIPD;
        QMap<int, double> cdfCOUNT;
        QMap<int, QMap<int, double> > cdfWIN;

        changeToCDF(&IPDS, cdfIPD);
        changeToCDF(&COUNT, cdfCOUNT);
        for (auto key : WIN.keys()) {
            QMap<int, double> temp;
            changeToCDF(&WIN[key], temp);
            cdfWIN[key] = temp;
        }

        generateColPair(&this->parameter, &cdfWIN, "CDF", col_name, col_values);
        sql_cmd = QString("INSERT INTO `%1`.`cdf_%2` "
                          "(`SOURCEID`,`PARAID`,`IPDCDF`,`COUNTCDF`,%5) "
                          "VALUES ('%2','0','%3','%4',%6)").arg(
                          this->parameter["database_distribution"]).arg(id).arg(
                          praseMapToJSON(&cdfIPD)).arg(praseMapToJSON(&cdfCOUNT)).arg(
                          col_name).arg(col_values);

        if (!sql_distribution->exec(sql_cmd)) {
            logQueryError(sql_distribution);
        }

        QMap<int, double> pmfIPD;
        QMap<int, double> pmfCOUNT;
        QMap<int, QMap<int, double> > pmfWIN;

        changeToPMF(&IPDS, pmfIPD);
        changeToPMF(&COUNT, pmfCOUNT);
        for (auto key : WIN.keys()) {
            QMap<int, double> temp;
            changeToPMF(&WIN[key], temp);
            pmfWIN[key] = temp;
        }

        generateColPair(&this->parameter, &pmfWIN, "PMF", col_name, col_values);
        sql_cmd = QString("INSERT INTO `%1`.`pmf_%2` "
                          "(`SOURCEID`,`PARAID`,`IPDPMF`,`COUNTPMF`,%5) "
                          "VALUES ('%2','0','%3','%4',%6)").arg(
                          this->parameter["database_distribution"]).arg(id).arg(
                          praseMapToJSON(&pmfIPD)).arg(praseMapToJSON(&pmfCOUNT)).arg(
                          col_name).arg(col_values);

        if (!sql_distribution->exec(sql_cmd)) {
            logQueryError(sql_distribution);
        }
    }
    db_distribution->commit();


    QThreadPool pool(this);
    pool.setMaxThreadCount(this->parameter["THREADS"].toInt());
    log = QString("Thread Pool is resized to [%1]").arg(pool.maxThreadCount());
    qDebug() << log;
    ProcessOperation::logToFile(&log);

    for (auto table : *tables) {
        Task *task = new Task(&this->database, &this->parameter, table);
        pool.start(task);
        log = QString("Thread for %1 is actived.").arg(table);
        logToFile(&log);
    }

    pool.waitForDone();
    delete sql_distribution;
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
    if (!db_source->open()) {
        log = QString("Cannot open MySQL database in[%1]:[%2].\n").arg(__FILE__).arg(__LINE__) +
                db_source->lastError().text();
        logToFile(&log);
        qDebug() << log;
        return;
    } else {
        log = QString("MySQL database SQl is opened in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
        logToFile(&log);
        qDebug() << log;
    }
    QSqlQuery *sql_source = new QSqlQuery(*db_source);

    QList<int> types;
    QString typeCondition;
    QStringList typeList = QString(this->parameter["TYPE"]).split(',');
    for (auto type : typeList) {
        types.push_back(type.toInt());
        typeCondition += "`TYPE`='" + type + "'";
        if (typeList.indexOf(type) != typeList.length() - 1) {
            typeCondition += " OR ";
        }
    }

    sql_source->clear();
    QString sql_cmd = QString("SELECT `ID`,`MAXSEQ`,`TIMEMAP`,`LOSTLIST` FROM `%1`.`%2` WHERE %3").arg(
                              this->parameter["database_source"]).arg(
                              this->parameter["tablename_captured"]).arg(typeCondition);
    if (!sql_source->exec(sql_cmd)) {
        logQueryError(sql_source);
    }
    log.clear();
    while (sql_source->next()) {
        int ID = sql_source->value("ID").toInt();

        QMap<int, int> timeMap;
        QList<int> dropped;
        if (praseTimeMap(sql_source->value("TIMEMAP").toString(), timeMap) &&
                praseDropList(sql_source->value("LOSTLIST").toString(), dropped)) {
            g_captureIDTimeMap[ID] = timeMap;
            g_captureIDDropList[ID] = dropped;
            g_captureIDs.push_back(ID);
            g_capturedIDMAXSeq[ID] = sql_source->value("MAXSEQ").toInt();
        } else {
            log = QString("Cannot prase TimeMap of DropList of [%1], skipping.").arg(ID);
            ProcessOperation::logToFile(&log);
            qDebug() << log;
        }
    }
    sql_source->clear();

    QList <QString> targetTables;
    sql_cmd = QString("SELECT `table_name` FROM `information_schema`.`tables` WHERE "
                      "`table_schema`='%1' AND `table_type`='BASE TABLE';").arg(
                      this->parameter["database_modulation"]);
    ProcessOperation::logToFile(&sql_cmd);
    if (sql_source->exec(sql_cmd)) {
        while (sql_source->next()) {
            targetTables.push_back(sql_source->value("table_name").toString());
        }
    } else {
        logQueryError(sql_source);
    }

    log = QString("Target Table List Size = %1").arg(targetTables.size());
    ProcessOperation::logToFile(&log);
    qDebug() << log;

    sql_source->clear();
    delete sql_source;
    db_source->close();

    processAction(&targetTables);
    preDestroy();
}
