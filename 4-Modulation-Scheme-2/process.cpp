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
static QMap<int, int> g_captureSSRC;
static bool isDEBUG;
static QString g_hashSalt;
static int g_randomSalt;


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
                log = sql_source->lastError().text() + ". " + sql_source->lastQuery();
                logToFile(&log);
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

    isDEBUG = this->parameter["DEBUG"].toInt();
    g_hashSalt = this->parameter["HASHSALT"];
    g_randomSalt = this->parameter["RANDOMSALT"].toInt();

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


// split the covert message
void dataSpilit(int dataID, int Ld, int groups, QList<QString> &result) {
    QString *targetData = &g_dataMap[dataID];

    int baseIndex = 0;
    QString log;

    while (groups > 0) {
        // Step a. get substring of data
        QStringRef subString(targetData, baseIndex, Ld);
        result.push_back(subString.toString());

        // Step b. increase index
        baseIndex += Ld;
        -- groups;

        if (isDEBUG) {
            log += subString + ",";
        }
    }

    if (isDEBUG) {
        ProcessOperation::logToFile(&log);
    }
}


// append hash section in each codeword
void appendHashSections(QList<QString> *source, int Lhash, int R, QList< QList<QString> > &result) {
    QString tempStr, log;

    // Step a. judge group number
    if (source->size() / R <= 0) {
        result.clear();
        return;
    }

    // Step b. generate sub QList for each group
    for (int i = 0 ; i < source->size() / R ; ++i) {
        QList<QString> temp;
        result.push_back(temp);
    }
    if (source->size() % R != 0) {
        QList<QString> temp;
        result.push_back(temp);
    }

    // Step c. loop the data substring, generate the codewords for each group
    QList<QString> *appendTarget = &result[0];
    for (int i = 0 ; i < source->size() ; ++i) {
        // Step d. check if the group come to R, clear tempStr and move append pointer to next
        if (i % R == 0) {
            tempStr.clear();
            appendTarget = &result[i / R];
        }
        // Step e. append data and calculate hash result
        tempStr += source->at(i);
        QString source_str = QString("%1%2%1").arg(g_hashSalt).arg(tempStr);
        QString hash_result = QCryptographicHash::hash(source_str.toLocal8Bit(),
                                                       QCryptographicHash::Sha1).toHex();
        QStringRef subResult(&hash_result, 8, sizeof(int) * 2 - 1);

        // Step f. change format from hex string to binary string
        int interger_result = subResult.toInt(nullptr, 16) % (1 << Lhash);
        QString append_result = QString::number(interger_result, 2);

        // Step g. append '0' to fromt, in case of length missing
        while (append_result.length() < Lhash) {
            append_result.push_front('0');
        }

        // Step h. append binary hash string to the end of data
        QString group_result = (*source)[i] + append_result;
        tempStr.push_back(append_result);

        // Step i. append codeword to the group list
        appendTarget->push_back(group_result);

        if (isDEBUG) {
            log = QString("group[%1] \t HASH(%2) = [(%3)16, (%4)2]").arg(i).arg(
                        tempStr).arg(subResult.toString()).arg(append_result);
            ProcessOperation::logToFile(&log);
        }
    }
}


// append the hash section in group R
void appendRVerifications(QList< QList<QString> > &source, int length) {

    // Step a. Loop for each group
    int group = 0;
    for (auto it = source.begin() ; it != source.end() ; ++ it, ++ group) {

        QString dataSpilice;
        dataSpilice.clear();

        // Step b. loop the data string in the group, then appeng to the end
        for (auto data = it->begin() ; data != it->end() ; ++ data) {
            dataSpilice += *data;
        }

        // Step c. calculate hash digest, get hex substring
        QString hash_result = QCryptographicHash::hash(g_hashSalt.toLocal8Bit() + dataSpilice.toLocal8Bit() + g_hashSalt.toLocal8Bit(),
                                                       QCryptographicHash::Sha1).toHex();
        QStringRef subResult(&hash_result, 8, sizeof(int) * 2 - 1);

        // Step d. change format from hex string to binary string
        int interger_result = subResult.toInt(nullptr, 16) % (1 << length);
        QString append_result = QString::number(interger_result, 2);

        // Step e. append '0' to fromt, in case of length missing
        while (append_result.length() < length) {
            append_result = '0' + append_result;
        }

        // Step f. append string to the end
        it->push_back(append_result);

        if (isDEBUG) {
            QString log = QString("R for Group[%1]: \t hash(%2) = (%3)16, (%4)2").arg(group).arg(
                        dataSpilice).arg(subResult.toString()).arg(append_result);
            ProcessOperation::logToFile(&log);
        }
    }
}


// change the 2-D array into 1-D list
void changeArraytoList(QList< QList<QString> > *source, QList<QString> &result, int group_count) {
    result.clear();

    // Step a. for each group
    for (auto group = source->begin() ; group != source->end() ; ++ group) {

        // Step b. for each string
        for (auto str = group->begin() ; str != group->end() ; ++ str) {
            result.push_back(*str);

            // Step c. judge the end
            if (result.size() >= group_count) {
                break;
            }
        }
    }
}


// append crc section for each codeword
void appendCRCSections(QList<QString> &source, int Lcrc) {

    // Step a. loop for each codeword
    int group = 0;
    for (auto codeword = source.begin() ; codeword != source.end() ; ++ codeword, ++ group) {

        // Step b. generate checksum
        QString check_source = QString("%1%2%1").arg(g_hashSalt).arg(*codeword);
        QString crc_result = QCryptographicHash::hash(check_source.toLocal8Bit(), QCryptographicHash::Md5).toHex();
        QStringRef subResult(&crc_result, 8, sizeof(int) * 2 - 1);

        // Step c. change format from hex string to binary string
        int interger_result = subResult.toInt(nullptr, 16) % (1 << Lcrc);
        QString append_result = QString::number(interger_result, 2);

        // Step d. append '0' to fromt, in case of length missing
        while (append_result.length() < Lcrc) {
            append_result = '0' + append_result;
        }

        if (isDEBUG) {
            QString log = QString("codeword[%1]: \tCRC(%2) = (%3)16, (%4)2").arg(
                        group).arg(*codeword).arg(subResult.toString()).arg(append_result);
            ProcessOperation::logToFile(&log);
        }

        // Step e. append the checksum to the end of codeword
        codeword->append(append_result);
    }
}


void changeBinaryToNumbers(QList<QString> *source, QList<int> &result) {

    // Step a. loop for each codeword
    int group = 0;
    for (auto codeword = source->begin() ; codeword != source->end() ; ++ codeword, ++ group) {

        // Step b. change from binary string into symbol starting from 1
        int symbol = codeword->toInt(nullptr, 2) + 1;

        // Step c. append symbol to 1-D result
        result.push_back(symbol);

        if (isDEBUG) {
            QString log = QString("group[%1]: \t (%2)2->(%3)10").arg(
                        group).arg(*codeword).arg(symbol);
            ProcessOperation::logToFile(&log);
        }
    }
}


// add random offset, the symbols start from 1
void introduceRandomOffset(QList<int> &source, int random_seed, int Lcodeword) {

    // Step a. init the random generator, the seed is composed of salt and extracted seed
    qsrand(quint32(random_seed) | quint32(g_randomSalt));
    int symbolMax = 1 << Lcodeword;

    // Step b. loop for each symbol
    int group = 0;
    for (auto symbol = source.begin() ; symbol != source.end() ; ++ symbol, ++ group) {

        // Step c. add random offset
        int offset = 4096 + qrand() % 4096;
        int symbol_with_offset = ((*symbol - 1) + offset) % symbolMax + 1;

        if (isDEBUG) {
            QString log = QString("group[%1]: \t origin(%2)->offset(%3)").arg(
                        group).arg(*symbol).arg(symbol_with_offset);
            ProcessOperation::logToFile(&log);
        }

        // Step d. replace value
        *symbol = symbol_with_offset;
    }
}


// add xor result in Mcols at the matrix
void appendXorVerification(QList<int> *source, QList<int> &result) {
    // Step a. init xor result
    int xor_result = 0;

    // Step b. loop for each symbols
    for (int group = 0 ; group < source->size() ; ++ group) {
        // Step c. dirext append to the end
        int symbol = source->at(group);
        result.push_back(symbol);

        // Step d. xor calculation
        xor_result ^= symbol - 1;

        // Step e. for 1,3,5(from 0) group, append xor result as Mcols, and reset xor_result
        if ((group + 1) % 2 == 0) {
            xor_result += 1;
            if (isDEBUG) {
                QString log = QString("group[%1]: \t Xor=(%2)").arg(group + 1).arg(xor_result);
                ProcessOperation::logToFile(&log);
            }

            result.push_back(xor_result);
            xor_result = 0;
        }
    }
}


// map symbols in each group into sequence numbers
void mapGroupSymbolsToSequenceNumbers(QList<int> *source, QList<int> &result, int Lcodeword, int Mcols) {
    // Step a. calculate the matrix size;
    int packets_per_codeword = 1 << Lcodeword;
    int packets_per_matrix = packets_per_codeword * Mcols;

    // Step b. loop for each group
    for (int group = 0 ; group < source->size() ; ++group) {

        // Step c. calculate for group ID
        int matrix_number = group / Mcols;
        int symbol = source->at(group);

        // Step d. calculate the offset in the group
        int offset_in_matrix = (symbol - 1) * Mcols + group % Mcols + 1;

        // Step e. generate sequence number, composed of offset in group and former sum of packets
        int sequence_number = matrix_number * packets_per_matrix + offset_in_matrix;

        // Step f. push result
        result.push_back(sequence_number);

        if (isDEBUG) {
            QString log = QString("group[%1]: \t symbol(%2)->seq_num(%3)").arg(group).arg(symbol).arg(sequence_number);
            ProcessOperation::logToFile(&log);
        }
    }
}


QString praseIntListToCSV(QList<int> *source) {
    QString result;

    for (auto item : *source) {
        result += QString::number(item);
        if (source->indexOf(item) != source->size() - 1) {
            result += ",";
        }
    }

    return result;
}


QString modulation(int source_id, int data_id, int Lcodeword, int Lhash, int Lcrc, int R, int Mcols, int &sent_bits) {
    int max_sequence_number = g_captureLengthMap[source_id];
    int count_per_matrix = (1 << Lcodeword) * Mcols;
    int max_covert_groups = (max_sequence_number / count_per_matrix) * ((Mcols / 3) * 2);
    int verification_periods = max_covert_groups / (R + 1);
    if (max_covert_groups <= 0 || Lcodeword - Lhash - Lcrc <= 0 || verification_periods <= 0) {
        sent_bits = 0;
        QString log = QString("source[%1], data[%2], Lcodeword[%3], Lhash[%4], Lcrc[%5], R[%6], Mcols[%7] "
                              "Failed, with groups[%8], mas_seq[%9].").arg(source_id).arg(data_id).arg(Lcodeword).arg(Lhash).arg(
                              Lcrc).arg(R).arg(Mcols).arg(max_covert_groups).arg(max_sequence_number);
        ProcessOperation::logToFile(&log);
        return "";
    }

    // Step 1. split the covert message, into data blocks
    sent_bits = (max_covert_groups - verification_periods) * (Lcodeword - Lcrc - Lhash);
    QList<QString> covert_message_binary_strs;
    Q_ASSERT(Lcodeword - Lhash - Lcrc > 0 && verification_periods != 0);
    dataSpilit(data_id, Lcodeword - Lhash - Lcrc, max_covert_groups - verification_periods, covert_message_binary_strs);

    // Step 2. append the HASH sectionn into the binary string
    QList< QList<QString> > group_message_strs;
    Q_ASSERT(covert_message_binary_strs.size());
    appendHashSections(&covert_message_binary_strs, Lhash, R, group_message_strs);
    covert_message_binary_strs.clear();

    // Step 3. generate the binary string of R groups
    Q_ASSERT(group_message_strs.size());
    appendRVerifications(group_message_strs, Lcodeword - Lcrc);

    // Step 4. change 2-D to 1-D
    QList<QString> codeword_strs;
    Q_ASSERT(group_message_strs.size());
    changeArraytoList(&group_message_strs, codeword_strs, max_covert_groups);
    group_message_strs.clear();

    // Step 5. append the CRC section into the binary string
    Q_ASSERT(codeword_strs.size());
    appendCRCSections(codeword_strs, Lcrc);

    // Step 6. change the binary string(start from 0) into symbols(start from 1)
    QList<int> group_symbols;
    Q_ASSERT(codeword_strs.size());
    changeBinaryToNumbers(&codeword_strs, group_symbols);
    group_message_strs.clear();

    // Step 7. introduce random offset to each group
    Q_ASSERT(group_symbols.size());
    introduceRandomOffset(group_symbols, g_captureSSRC[source_id], Lcodeword);

    // Step 8. add xor verification to Mcols in the matrix
    QList<int> group_symbols_with_xor;
    Q_ASSERT(group_symbols.size());
    appendXorVerification(&group_symbols, group_symbols_with_xor);
    group_symbols.clear();

    // Step 9. map the symbols into sequence numbers
    QList<int> dropoutSequenceNumbers;
    Q_ASSERT(group_symbols_with_xor.size());
    mapGroupSymbolsToSequenceNumbers(&group_symbols_with_xor, dropoutSequenceNumbers, Lcodeword, Mcols);
    std::sort(dropoutSequenceNumbers.begin(), dropoutSequenceNumbers.end());
    group_symbols_with_xor.clear();

    // Step 9. change list to csv string
    QString sequence_str = praseIntListToCSV(&dropoutSequenceNumbers);
    return sequence_str;
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

    QSqlQuery *sql_source = new QSqlQuery(QSqlDatabase::database(con_source));
    QSqlQuery *sql_modulation = new QSqlQuery(QSqlDatabase::database(con_modulation));

    int Lcodeword = 0, R = 0, Lhash = 0, Lcrc = 0, Mcols = 0;
    QString sql_cmd = QString("SELECT `LCODEWORD`,`LHASH`,`LCRC`,`R`,`MCOLS` FROM `%1`.`%2` WHERE `ID`='%3'").arg(
                this->database->databases["source"]).arg((*this->parameter)["tablename_parameters"]).arg(
                this->table);

    if (sql_source->exec(sql_cmd)) {
        if (sql_source->next()) {
            Lcodeword = sql_source->value("LCODEWORD").toInt();
            R = sql_source->value("R").toInt();
            Lhash = sql_source->value("LHASH").toInt();
            Lcrc = sql_source->value("LCRC").toInt();
            Mcols = sql_source->value("MCOLS").toInt();
        }

        log = QString("Table [%1], Lcodeword=%2, Lhash=%3, Lcrc=%4, R=%5, Mcols=%6, "
                      "size(DATA)=%7, size(CAPTURE)=%8").arg(this->table).arg(
                      Lcodeword).arg(Lhash).arg(Lcrc).arg(R).arg(Mcols).arg(
                      g_dataIDs.size()).arg(g_captureIDs.size());
        ProcessOperation::logToFile(&log);

        for (auto capture : g_captureIDs) {
            for (auto data : g_dataIDs) {
                int sent_bits = 0;
                QString drop_list = modulation(capture, data, Lcodeword, Lhash, Lcrc, R, Mcols, sent_bits);

                if (sent_bits) {
                    sql_cmd = QString("INSERT INTO `%1`.`%2`(`PARAID`,`SOURCEID`,`DATAID`,`DROPLIST`,`SENTBITS`) "
                                      "VALUES('%3','%4','%5','%6','%7')").arg(this->database->databases["modulation"]
                                      ).arg(this->table).arg(this->table.toInt()).arg(capture).arg(data).arg(drop_list).arg(sent_bits);

                    if (!sql_modulation->exec(sql_cmd)) {
                        logQueryError(sql_modulation);
                    }
                }
            }
        }
    } else {
        logQueryError(sql_source);
    }

    sql_source->clear();
    sql_modulation->clear();
    sql_source->finish();
    sql_modulation->finish();

    delete sql_source;
    delete sql_modulation;

    log = QString("Thread for [%1] finished.").arg(this->table);
    ProcessOperation::logToFile(&log);
    qDebug() << log;
}

void ProcessOperation::processAction(QList<QString> *tables) {
    QString log = QString("processAction function is called in[%1]:[%2].").arg(__FILE__).arg(__LINE__);
    logToFile(&log);

    //@TODO
    QThreadPool pool(this);
    if (isDEBUG) {
        pool.setMaxThreadCount(1);
    } else {
        pool.setMaxThreadCount(this->parameter["THREADS"].toInt());
    }

    log = QString("Thread Pool is actually [%1].").arg(pool.maxThreadCount());
    qDebug() << log;
    logToFile(&log);

    for (auto table : *tables) {
        Task *task = new Task(&this->database, &this->parameter, table);
        pool.start(task);
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
    sql_cmd = QString("SELECT `ID`,`MAXSEQ`,`SSRC` FROM `%1`.`%2` WHERE %3").arg(this->database.databases["source"]).arg(
                this->parameter["tablename_captured"]).arg(typesCondition);
    g_captureIDs.clear();
    g_captureLengthMap.clear();
    if (sql_source->exec(sql_cmd)) {
        while (sql_source->next()) {
            int id = sql_source->value("ID").toInt();
            g_captureIDs.push_back(id);
            g_captureLengthMap[id] = sql_source->value("MAXSEQ").toInt();
            g_captureSSRC[id] = sql_source->value("SSRC").toInt();
        }
    } else {
        log = sql_source->lastError().text() + ". " + sql_source->lastQuery();
        ProcessOperation::logToFile(&log);
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
