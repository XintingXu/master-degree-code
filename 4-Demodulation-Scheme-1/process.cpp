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

static QMap<int, QString> g_dataMap;
static QMap<int, QList<int> > g_sourceMap;
#define MAXINMATRIX 256
static bool isDebugging = false;

bool ProcessOperation::init() {
    QString log = QString("init function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    QSqlDatabase *db_modulation = this->dbs[this->parameter["database_modulation"]];
    if (db_modulation->isOpen()) {
        QSqlQuery *sql_modulation = new QSqlQuery(*db_modulation);

        QString sql_cmd = QString("SELECT CONCAT(\"DROP VIEW IF EXISTS `%1`.`\", table_name, \"`;\") "
                                  "FROM `information_schema`.`tables` WHERE TABLE_SCHEMA='%1' AND TABLE_NAME LIKE 'result%';").arg(
                                  this->database.databases["modulation"]);
        logToFile(&sql_cmd);

        if (!sql_modulation->exec(sql_cmd)) {
            log = sql_modulation->lastError().text() + ". " + sql_modulation->lastQuery();
            qDebug() << log;
            logToFile(&log);
            sql_modulation->clear();
            delete sql_modulation;
            return false;
        }

        sql_cmd.clear();
        while (sql_modulation->next()) {
            sql_cmd += sql_modulation->value(0).toString();
        }
        if (sql_cmd.length()) {
            logToFile(&sql_cmd);
            if (!sql_modulation->exec(sql_cmd)) {
                logQueryError(sql_modulation);
            }
        }

        sql_modulation->clear();
        delete sql_modulation;
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

    QString sql_cmd = QString("UPDATE `%1`.`%2` SET `%2`.`DEMODULATED`=NULL,`%2`.`ERRORBITS`=0;").arg(
                (*this->parameter)["database_modulation"]).arg(this->table);
    if (!sql_modulation->exec(sql_cmd)) {
        log = QString("Cannot reset table `%1`\n").arg(this->table) +
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

void seperateSequenceIntoGroups(QList<int> *source, int R, int matrixSize, QList< QList<int> > &result) {
    int packets_per_matrix = matrixSize * matrixSize;
    int packets_per_period = packets_per_matrix * (R + 1);

    // seperate the sequence numbers into different matrixs
    // the group number is calculated by dividing the matrix size
    // the remainder is the relative result in the matrix.
    // However, the sequence number starts from 1, so the group number should -1 first
    // As for the remainder, mod result ends with 0, but the reverse mapping requires endding with packets_per_matrix
    for (int i = 0 ; i < source->size() ; ++i) {
        int data = source->at(i) - 1;
        int group = data / packets_per_period;
        if (group + 1 > result.size()) {
            QList<int> temp;
            result.push_back(temp);
        }
        result[group].push_back(data % packets_per_period + 1);
    }

    for (int i = 0 ; i < result.size() ; ++i) {
        std::sort(result[i].begin(), result[i].end());

        QString log = QString("Sequence Numbers for group[%1] : ").arg(i + 1);
        for (int j = 0 ; j < result[i].size() ; ++j) {
            log += QString::number(result[i][j]) + ",";
        }
        if (isDebugging) {
            ProcessOperation::logToFile(&log);
        }
    }
}

QString generateChecksum(QList<int> *source, int Lcodeword, int &checksum) {
    QString result("");
    for (int i = 0 ; i < source->size() ; ++i) {
        QString temp(QString::number(source->at(i), 2));
        while (temp.length() < Lcodeword) {
            temp = "0" + temp;
        }
        result += temp;
    }
    QString hashResult = QCryptographicHash::hash(result.toLocal8Bit(), QCryptographicHash::Sha1).toHex();
    QStringRef subResult(&hashResult, 8, sizeof(int) * 2 - 1);
    checksum = subResult.toInt(nullptr, 16) % (2 << (Lcodeword - 1));

    QString log = QString("Checksum of (%1) = %2[%3];").arg(result).arg(checksum).arg(subResult);
    if (isDebugging) {
        ProcessOperation::logToFile(&log);
    }

    return result;
}

QString demodulatePerGroup(QList< QList<int> > *source, int Lcodeword, int R, int matrixSize, QMap<int, int> *mappingMatrix) {
    int packets_per_matrix = matrixSize * matrixSize;

    QString result("");

    // for each group, the demodulation operation is executed seperately
    // values in source starts from 1
    for (int group = 0 ; group < source->size() ; ++ group) {
        QList< QList<int> > combinations;
        QList< QList<int> > packetInGroups;
        QSet<int> checksum;

        int index = 0;
        for (int matrix = 0 ; matrix < R ; ++ matrix) {
            QList<int> inGroup;
            int matrixBegin = matrix * packets_per_matrix;
            int matrixEnd = matrixBegin + packets_per_matrix;
            int countInMatrix = 0;

            while (index < source->at(group).size()) {
                int data = source->at(group).at(index) - 1;
                if (data >= matrixBegin && data < matrixEnd) {
                    if (countInMatrix < MAXINMATRIX) {
                        inGroup.push_back(data - matrixBegin + 1);
                        ++ countInMatrix;
                    }
                    ++index;
                } else {
                    break;
                }
            }
            packetInGroups.push_back(inGroup);
        }

        int sourceMax = 2 << (Lcodeword - 1);
        while (index < source->at(group).size()) {
            int data = source->at(group).at(index) - 1;
            int source = (*mappingMatrix)[(data % packets_per_matrix) + 1];
            if (source <= sourceMax) {
                checksum.insert(source);
            }
            ++ index;
        }
        // reverse mapping, the sequence number starts from 1 is mapped into data value start from 0
        QList< QList<int> > packetInGroupsTemp;
        for (int i = 0 ; i < packetInGroups.size() ; ++i) {
            QList <int> tempForGroup;
            for (int j = 0 ; j < packetInGroups.at(i).size() ; ++j) {
                int temp = (packetInGroups[i][j] - 1) % packets_per_matrix + 1;
                int source = (*mappingMatrix)[temp];
                if (source < sourceMax) {
                    tempForGroup.push_back(source);
                }
            }
            packetInGroupsTemp.push_back(tempForGroup);
        }
        packetInGroups.clear();
        packetInGroups = packetInGroupsTemp;
        packetInGroupsTemp.clear();

        for (int i = 0 ; i < packetInGroups.size() ; ++i) {
            QList< QList<int> > lastGroup = combinations;
            combinations.clear();

            for (int j = 0 ; j < packetInGroups.at(i).size() ; ++j) {
                if (i == 0) {
                    QList<int> temp;
                    temp.push_back(packetInGroups.at(i).at(j));
                    combinations.push_back(temp);
                } else {
                    for (int item = 0 ; item < lastGroup.size() ; ++item) {
                        QList<int> combine = lastGroup.at(item);
                        combine.push_back(packetInGroups.at(i).at(j));
                        combinations.push_back(combine);
                    }
                }
            }
        }

        for (int i = 0 ; i < packetInGroups.size() ; ++i) {
            QString log = QString("combination (%1) is : ").arg(i);
            for (int j = 0 ; j < packetInGroups.at(i).size() ; ++j) {
                log += QString::number(packetInGroups[i][j]) + ",";
            }
            if (isDebugging) {
                ProcessOperation::logToFile(&log);
            }
        }

        QString log("Checksum candidates : ");
        for (auto item :checksum) {
            log += QString::number(item) + ",";
        }
        if (isDebugging) {
            ProcessOperation::logToFile(&log);
        }

        bool demodulateSuccess = false;
        for (int i = 0 ; i < combinations.size() ; ++i) {
            int checkResult = 0;
            QString subString = generateChecksum(&combinations[i], Lcodeword, checkResult);
            if (checksum.find(checkResult) != checksum.end()) {
                result += subString;
                demodulateSuccess = true;
                break;
            }
        }
        if (!demodulateSuccess) {
            QString pending;
            for (int i = 0 ; i < R * Lcodeword ; ++i) {
                pending += "0";
            }
            result += pending;
        }
    }

    return result;
}

QString demodulation(int sourceID, int dataID, int Lcodeword, int R, int sent_bits, QString *modulated, QMap<int, int> *mappingMatrix, int &error_bits) {
    int mapping_matrix_size = static_cast<int>(pow(2 << (Lcodeword - 1), 0.5));
    if (mapping_matrix_size * mapping_matrix_size < 2 << (Lcodeword - 1)) {
        mapping_matrix_size += 1;
    }

    error_bits = 0;
    QStringList dropStringList = modulated->split(',');
    QList<int> dropSequenceList;
    for (auto packet : dropStringList) {
        dropSequenceList.push_back(packet.toInt());
    }
    if (g_sourceMap.find(sourceID) != g_sourceMap.end()) {
        dropSequenceList += g_sourceMap[sourceID];
    }

    QList< QList<int> > packetGroups;
    seperateSequenceIntoGroups(&dropSequenceList, R, mapping_matrix_size, packetGroups);

    QString demodulated = demodulatePerGroup(&packetGroups, Lcodeword, R, mapping_matrix_size, mappingMatrix);

    if (demodulated.length() < sent_bits) {
        error_bits = sent_bits - demodulated.length();
    }
    for (int i = 0 ; i < std::min(demodulated.length(), g_dataMap[dataID].length()) ; ++i) {
        if (demodulated[i] != g_dataMap[dataID][i]) {
            ++ error_bits;
        }
    }

    QString log = "Demodulated : " + demodulated;
    if (isDebugging) {
        ProcessOperation::logToFile(&log);
    }

    return demodulated;
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

/*
 modulation maps the data into sequence number;
 demodulation maps the sequence number into data;
 those are inverse operations
*/
        result[seq] = data; // this is different from modulation

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

        log = QString("Table [%1], Lcodeword=%2, R=%3, size(DATA)=%4").arg(this->table).arg(
                    Lcodeword).arg(R).arg(g_dataMap.size());
        ProcessOperation::logToFile(&log);

        QMap<int, int> mappingMatrix;
        generateMappingMatrix(mappingMatrix, Lcodeword);

        QMap<int, int> modulationIDDATA;
        QMap<int, int> modulationIDSOURCE;
        QMap<int, int> modulationIDSentBits;

        sql_cmd = QString("SELECT `ID`,`DATAID`,`SOURCEID`,`SENTBITS` FROM `%1`.`%2`;").arg(this->database->databases["modulation"]).arg(
                    this->table);
        if (sql_modulation->exec(sql_cmd)) {
            while (sql_modulation->next()) {
                int ID = sql_modulation->value("ID").toInt();
                modulationIDDATA[ID] = sql_modulation->value("DATAID").toInt();
                modulationIDSOURCE[ID] = sql_modulation->value("SOURCEID").toInt();
                modulationIDSentBits[ID] = sql_modulation->value("SENTBITS").toInt();
            }
        } else {
            log = sql_modulation->lastError().text() + ", " + sql_modulation->lastQuery();
            ProcessOperation::logToFile(&log);
        }

        sql_cmd.clear();
        for (auto id = modulationIDDATA.begin() ; id != modulationIDDATA.end() ; ++id) {
            int error_bits = 0;
            log = QString("Start Demodulating ID [%1]").arg(id.key());
            ProcessOperation::logToFile(&log);

            QString modulated = QString("SELECT `DROPLIST` FROM `%1`.`%2` WHERE `ID`='%3';").arg(
                        this->database->databases["modulation"]).arg(this->table).arg(id.key());

            if (!sql_modulation->exec(modulated)) {
                log = sql_modulation->lastError().text() + ". " + sql_modulation->lastQuery();
                ProcessOperation::logToFile(&log);
                modulated.clear();
            } else {
                if (sql_modulation->next()) {
                    modulated = sql_modulation->value(0).toString();
                } else {
                    modulated.clear();
                }
            }

            QString demodulated = demodulation(modulationIDSOURCE[id.key()], id.value(), Lcodeword, R,
                    modulationIDSentBits[id.key()], &modulated, &mappingMatrix, error_bits);

            sql_cmd = QString("UPDATE `%1`.`%2` SET `DEMODULATED`='%3',`ERRORBITS`='%4' WHERE `ID`='%5';").arg(
                        this->database->databases["modulation"]).arg(this->table).arg(demodulated).arg(
                        error_bits).arg(id.key());

            if (!sql_modulation->exec(sql_cmd)) {
                logQueryError(sql_modulation);
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

    QSqlDatabase *db_source = this->dbs[this->parameter["database_source"]];
    if (!db_source->isOpen() && !db_source->open()) {
        log = QString("Cannot open MySQL database in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
        logToFile(&log);
        qDebug() << log;
        log = db_source->lastError().text();
        qDebug() << log;
        logToFile(&log);
        return;
    } else {
        log = QString("MySQL database SQL is opened in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
        logToFile(&log);
        qDebug() << log;
    }
    QSqlQuery *sql_source = new QSqlQuery(*db_source);

    QList<QString> targetTables;

    QString sql_cmd = QString("SELECT table_name FROM `information_schema`.`tables` "
                              "WHERE TABLE_SCHEMA='%1'").arg(this->parameter["database_modulation"]);
    if (!sql_source->exec(sql_cmd)) {
        logQueryError(sql_source);
    }
    log.clear();
    while (sql_source->next()) {
        QString table = sql_source->value(0).toString();
        targetTables.push_back(table);
        log += table + ",";
    }
    sql_source->clear();

    log = QString("target Table length = %1").arg(targetTables.size());
    qDebug() << log;
    logToFile(&log);

    sql_source->clear();

    sql_cmd = QString("SELECT `ID`,`DATA` FROM `%1`.`%2`;").arg(
                this->parameter["database_source"]).arg(
                this->parameter["tablename_data"]);
    if (sql_source->exec(sql_cmd)) {
        while (sql_source->next()) {
            int id = sql_source->value("ID").toInt();
            QString data = sql_source->value("DATA").toString();
            g_dataMap[id] = data;
        }
    } else {
        logQueryError(sql_source);
    }

    sql_cmd = QString("SELECT `ID`,`LOSTLIST` FROM `%1`.`%2`;").arg(
                this->parameter["database_source"]).arg(this->parameter["tablename_captured"]);
    if (sql_source->exec(sql_cmd)) {
        while (sql_source->next()) {
            int ID = sql_source->value("ID").toInt();
            QString dropList = sql_source->value("LOSTLIST").toString();
            QList<int> droppedPackets;
            ProcessOperation::jsonStringToDropList(&dropList, droppedPackets);
            if (1) {
                g_sourceMap[ID] = droppedPackets;
            }
        }
    } else {
        logQueryError(sql_source);
    }

    processAction(&targetTables);

    //add create view option
    sql_cmd = "CREATE VIEW result_full AS ";
    for (auto table : targetTables) {
        QString temp = QString("(SELECT `PARAID` as `PARAID`,`ID` as `ID`,"
                               "`SOURCEID` as `SOURCEID`,"
                       "`DATAID` as `DATAID`,`SENTBITS` as `SENTBITS`,"
                       "`ERRORBITS` as `ERRORBITS` FROM `%1`.`%2`) ").arg(
                       this->parameter["database_modulation"]).arg(table);
        if (targetTables.indexOf(table) != targetTables.size() - 1) {
            temp += "UNION ";
        }
        sql_cmd += temp;
    }
    sql_cmd += ";";

    //ProcessOperation::logToFile(&sql_cmd);
    QSqlDatabase *db_modulation = this->dbs[this->parameter["database_modulation"]];
    if (!db_modulation->isOpen() &&!db_modulation->open()) {
        log = QString("Cannot open MySQL database in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
        logToFile(&log);
        qDebug() << log;
        log = db_modulation->lastError().text();
        qDebug() << log;
        logToFile(&log);
    }

    if (db_modulation->isOpen()) {
        QSqlQuery *sql_modulation = new QSqlQuery(*db_modulation);

        if (!sql_modulation->exec(sql_cmd)) {
            log = sql_modulation->lastError().text() + ", " + sql_modulation->lastQuery();
            ProcessOperation::logToFile(&log);
        }
        sql_modulation->clear();
        delete sql_modulation;
    }

    db_modulation->commit();
    db_modulation->close();
    db_source->close();
    delete sql_source;

    preDestroy();
}
