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
#include <QtConcurrent>
#include <QStringRef>
#include <QCryptographicHash>
#include <QPoint>
#include <QHash>

static QList<int> g_captureIDs;
static QList<int> g_dataIDs;
static QMap<int, QString> g_dataMap;
static QMap<int, int> g_captureLengthMap;

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

void dataSpilit(int dataID, int Lcodeword, int groups, QList<QString> &result) {
    QString *targetData = &g_dataMap[dataID];

    int baseIndex = 0;
    QString log;

    while (groups > 0) {
        QStringRef subString(targetData, baseIndex, Lcodeword);
        result.push_back(subString.toString());

        baseIndex += Lcodeword;
        -- groups;

        log += subString + ",";
    }

    //ProcessOperation::logToFile(&log);
}

void dataChecksum(QList<QString> *source, QList<int>&result, int R) {
    int count = 0;
    QString temp("");

    for (auto item : *source) {
        result.push_back(item.toInt(nullptr, 2));
        temp += item;
        ++ count;

        if (count == R) {
            QString hash_result = QCryptographicHash::hash(temp.toLocal8Bit(), QCryptographicHash::Sha1).toHex();
            QStringRef subResult(&hash_result, 8, sizeof(int) * 2 - 1);
            result.push_back(subResult.toInt(nullptr, 16) % (2 << (temp.length() / R - 1)));

            temp = "Checksum of (" + temp + ") = " + subResult + "[" + QString::number(result.last()) + "];";
            //ProcessOperation::logToFile(&temp);

            temp.clear();
            count = 0;
        }
    }
}

void mapSequenceNumber(QList<int> *source, QMap<int, int> *mappingMatrix, QList<int>&result, int matrixSize) {
    int matrixElements = matrixSize * matrixSize;
    for (int i = 0 ; i < source->size() ; ++i) {
        int seq = 0;
        int data = source->at(i);
        if (mappingMatrix->find(data) == mappingMatrix->end()) {
            QString log = QString("Cannot find Sequence number for data[%1]").arg(data);
            ProcessOperation::logToFile(&log);
        } else {
            seq = mappingMatrix->value(data) + i * matrixElements;

            QString log = "Map [" + QString::number(data) + "] into [" + QString::number(seq) + "], relative(" +
                    QString::number(mappingMatrix->value(data)) + ");";
            //ProcessOperation::logToFile(&log);
        }
        result.push_back(seq);
    }
}

QString modulation(int sourceID, int dataID, int Lcodeword, int R, QMap<int, int> *mappingMatrix,int &sent_bits) {
    int mapping_matrix_size = static_cast<int>(pow(2 << (Lcodeword - 1), 0.5));
    if (mapping_matrix_size * mapping_matrix_size < 2 << (Lcodeword - 1)) {
        mapping_matrix_size += 1;
    }

    int packets_per_matrix = mapping_matrix_size * mapping_matrix_size;
    int packets_per_period = packets_per_matrix * (R + 1);
    int availabel_data_groups = (g_captureLengthMap[sourceID] / packets_per_period) * R;
    sent_bits = availabel_data_groups * Lcodeword;

    QList<QString> sourceData;
    dataSpilit(dataID, Lcodeword, availabel_data_groups, sourceData);

    QList<int> integerData;
    dataChecksum(&sourceData, integerData, R);

    QList<int> sequenceNumber;
    mapSequenceNumber(&integerData, mappingMatrix, sequenceNumber, mapping_matrix_size);

    std::sort(sequenceNumber.begin(), sequenceNumber.end());

    QString result("");
    for (int i = 0 ; i < sequenceNumber.size() - 1 ; ++i) {
        result += QString::number(sequenceNumber[i]) + ",";
    }
    if (sequenceNumber.size()) {
        result += QString::number(sequenceNumber[sequenceNumber.size() - 1]);
    }

    return result;
}

void generateMappingMatrix(QMap<int, int> &result, int Lcodeword) {
    int mapping_matrix_size = static_cast<int>(pow(2 << (Lcodeword - 1), 0.5));
    if (mapping_matrix_size * mapping_matrix_size < (2 << (Lcodeword - 1))) {
        mapping_matrix_size += 1;
    }

    QString log = QString("Mapping Matrix size is %1 x %1").arg(mapping_matrix_size);
    ProcessOperation::logToFile(&log);

    QPoint point(0, 0);
    int seq = 0;
    bool direction = true;  // true for up-right, false for down-left
    while (point.x() < mapping_matrix_size && point.y() < mapping_matrix_size) {
        ++ seq;
        int data = point.x() * mapping_matrix_size + point.y();
        result[data] = seq;

        if (direction) {
            if (point.x() == 0 || point.y() == mapping_matrix_size - 1) {   //touch top or right
                direction = !direction;
                if (point.y() == mapping_matrix_size - 1) {
                    point.setX(point.x() + 1);
                } else {
                    point.setY(point.y() + 1);
                }
            } else {
                point.setX(point.x() - 1);
                point.setY(point.y() + 1);
            }
        } else {
            if (point.x() == mapping_matrix_size - 1 || point.y() == 0) {
                direction = !direction;
                if (point.x() == mapping_matrix_size - 1) {
                    point.setY(point.y() + 1);
                } else {
                    point.setX(point.x() + 1);
                }
            } else {
                point.setX(point.x() + 1);
                point.setY(point.y() - 1);
            }
        }
    }

    log.clear();
    for (auto it = result.begin() ; it != result.end() ; ++it) {
        log += "[" + QString::number(it.key()) + ": " + QString::number(it.value()) + "], ";
    }
    //ProcessOperation::logToFile(&log);
}

void Task::run() {
    QString log = QString("Thread is actived, table name :%1").arg(this->table);
    ProcessOperation::logToFile(&log);

    if (!preProcessDB()) {
        log = QString("Preprocess for Database is failed. table [%1]").arg(this->table);
        return;
    }

    QString con_source = this->database->databases["source"] + "_" +
            QString::number(uintptr_t(QThread::currentThreadId()));
    QString con_modulation = this->database->databases["modulation"] + "_" +
            QString::number(uintptr_t(QThread::currentThreadId()));

    QSqlQuery *sql_source = new QSqlQuery(QSqlDatabase::database(con_source));
    QSqlQuery *sql_modulation = new QSqlQuery(QSqlDatabase::database(con_modulation));

    int Lcodeword = 0, R = 0;
    QString sql_cmd = QString("SELECT `LCODEWORD`,`R` FROM `%1`.`%2` WHERE `ID`='%3'").arg(
                this->database->databases["source"]).arg((*this->parameter)["tablename_parameters"]).arg(
                this->table);

    if (sql_source->exec(sql_cmd)) {
        if (sql_source->next()) {
            Lcodeword = sql_source->value(0).toInt();
            R = sql_source->value(1).toInt();
        }

        log = QString("Table [%1], Lcodeword=%2, R=%3, size(DATA)=%4, size(CAPTURE)=%5").arg(this->table).arg(
                    Lcodeword).arg(R).arg(g_dataIDs.size()).arg(g_captureIDs.size());
        ProcessOperation::logToFile(&log);

        QMap<int, int> mappingMatrix;
        generateMappingMatrix(mappingMatrix, Lcodeword);

        for (auto capture : g_captureIDs) {
            for (auto data : g_dataIDs) {
                int sent_bits = 0;
                QString drop_list = modulation(capture, data, Lcodeword, R, &mappingMatrix, sent_bits);

                sql_cmd = QString("INSERT INTO `%1`.`%2`(`PARAID`,`SOURCEID`,`DATAID`,`DROPLIST`,`SENTBITS`) "
                                  "VALUES('%3','%4','%5','%6','%7')").arg(this->database->databases["modulation"]
                                  ).arg(this->table).arg(this->table.toInt()).arg(capture).arg(data).arg(drop_list).arg(sent_bits);

                if (!sql_modulation->exec(sql_cmd)) {
                    logQueryError(sql_modulation);
                }
            }
        }
    }

    sql_source->clear();
    sql_modulation->clear();
    sql_source->finish();
    sql_modulation->finish();

    delete sql_source;
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
    QList<int> types;
    QList<int> parameterIDs;

    sql_source->clear();

    QString sql_cmd = QString("SELECT `ID` FROM `%1`.`%2`;").arg(this->parameter["database_source"]).arg(
                    this->parameter["tablename_parameters"]);
    if (!sql_source->exec(sql_cmd)) {
        logQueryError(sql_source);
    }
    log.clear();
    while (sql_source->next()) {
        int id = sql_source->value(0).toInt();
        parameterIDs.push_back(id);
        log += QString::number(id) + ",";
    }
    log = "parameter ID : " + log;
    logToFile(&log);
    sql_source->clear();

    for (auto para : parameterIDs) {
        targetTables.push_back(QString("%1").arg(para));
    }

    log = QString("target Table length = %1").arg(targetTables.size());
    qDebug() << log;
    logToFile(&log);

    QString typesCondition;
    QStringList typeList = QString(this->parameter["TYPE"]).split(',');
    for (auto type : typeList) {
        types.push_back(type.toInt());
        typesCondition += "`TYPE`='" + type + "'";
        if (typeList.indexOf(type) != typeList.length() - 1) {
            typesCondition += " OR ";
        }
    }
    sql_cmd = QString("SELECT `ID`,`MAXSEQ` FROM `%1`.`%2` WHERE %3").arg(this->database.databases["source"]).arg(
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
    sql_cmd = QString("SELECT `ID`,`DATA` FROM `%1`.`%2`").arg(this->database.databases["source"]).arg(
                this->parameter["tablename_data"]);
    g_dataIDs.clear();
    g_dataMap.clear();
    if (sql_source->exec(sql_cmd)) {
        while (sql_source->next()) {
            int id = sql_source->value("ID").toInt();
            g_dataIDs.push_back(id);
            g_dataMap[id] = sql_source->value("DATA").toString();
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
