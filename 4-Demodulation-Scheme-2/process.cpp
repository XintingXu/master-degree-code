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
static QMap<int, int> g_captureSSRC;
static bool isDEBUG;
static QString g_hashSalt;
static int g_randomSalt;

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


// seperate the sequence numbers into each matrix, offset for each matrix is removed
void seperateSequenceNumbersMatrix(QList<int> *seq_numbers, QList< QList<int> > &results, int Lcodeword, int Mcols) {
    // Step a. calculate the size of group and matrix
    int packets_per_group = 1 << Lcodeword;
    int packets_per_matrix = packets_per_group * Mcols;

    // Step b. set list index and group index
    int matrix_id = 0;
    int index_begin = 0;

    // Step c. loop for each sequence number
    while (index_begin < seq_numbers->size()) {
        QList<int> group_result;

        // Step d. calculate the begin and end of each matrix
        int sequence_begin = matrix_id * packets_per_matrix + 1;
        int sequence_end = sequence_begin + packets_per_matrix;

        // Step e. collect sequence numbers for each matrix
        while (index_begin < seq_numbers->size()) {
            int seq = seq_numbers->at(index_begin);

            // Step f. judge whether the sequence number is in the range
            if (seq >= sequence_begin && seq < sequence_end) {
                // Step g. append the relative sequence number to the list, start from 1
                group_result.push_back(seq - sequence_begin + 1);

                // Step h. increase the index of source list
                ++ index_begin;
            } else {
                break;
            }
        }

        // Step i. push the list for matrix to result, and increase matrix ID for next
        results.push_back(group_result);
        ++ matrix_id;
    }

    if (isDEBUG) {
        QString log = "seperateSequenceNumbersMatrix result: ";
        for (auto matrix : results) {
            log += "[";
            for (auto item : matrix) {
                log += QString::number(item) + ",";
            }
            log += "], ";
        }
        ProcessOperation::logToFile(&log);
    }
}


// seperate the sequence numbers into each group, transform into symbols (start from 1)
void seperateMatrixToGroups(QList< QList<int> > *source, QList< QList< int > > &result, int Mcols) {
    // Step a. loop for each matrix
    for (auto matrix = source->begin() ; matrix != source->end() ; ++ matrix) {
        QList< QList<int> > matrix_result;

        // Step b. generate map for each group
        for (int i = 0 ; i < Mcols ; ++i) {
            QList<int> group_result;
            matrix_result.push_back(group_result);
        }

        // Step c. loop for each relative sequence number
        for (auto number = matrix->begin() ; number != matrix->end() ; ++ number) {
            // Step d. calculate the group id and symbol value
            int group_id = (*number - 1) % Mcols;
            int symbol = (*number - 1) / Mcols + 1;

            // Step e. push the symbol into map
            matrix_result[group_id].push_back(symbol);
        }

        // Step f. append result to the list
        for (auto group : matrix_result) {
            result.push_back(group);
        }
    }
}


// rearrange groups for xor verification per 2 message groups
void seperateGroupsForXor(QList< QList<int> > *groupList, QList< QList< QList<int> > > &xorResult) {
    // Step a. set xor verification period
    int xor_periods = 2;

    // Step b. set result for each xor period
    QList< QList<int> > perXor;

    // Step c. loop for each group
    for (int index = 0 ; index < groupList->size() ; ++ index) {

        // Step d. append group to xor period
        perXor.push_back(groupList->at(index));

        // Step e. if reaches period gap, append result and clear
        if ((index + 1) % (xor_periods + 1) == 0) {
            xorResult.push_back(perXor);
            perXor.clear();
        }
    }
}


// check XOR results recursively
void checkXORRecursively(QList< QList<int> > *dataArray,
                        QList< QMap<int, int> > &passedCount, int layer, QList<int> item, const int gap_count, int &current_count) {
    // Step a. judge whether is the last layer
    if (layer == dataArray->size() - 1) {
        // Step b. is if the last layer, judge the xor result, if count exceeds gap, exit
        if (current_count > gap_count) {
            return;
        }

        // Step c. calculate the xor result of former layers
        int xor_result = 0;
        for (auto data : item) {
            xor_result ^= data - 1;
        }

        // Step d. judge the final result with last layer
        for (auto it = dataArray->at(layer).begin() ; it != dataArray->at(layer).end() ; ++ it) {

            // Step e. calculate the final xor result
            int compare = xor_result ^ (*it - 1);

            // Step f. check the xor checksum whether correct
            if (compare == 0) {

                // Step g. increase the count of former items
                for (int index = 0 ; index < item.size() ; ++ index) {

                    // Step h. if the count item does not exists, create it
                    if (passedCount[index].find(item[index]) == passedCount[index].end()) {
                        passedCount[index][item[index]] = 0;
                    }

                    // Step i. increase the count value
                    passedCount[index][item[index]] += 1;
                }

                // Since Mcols means the xor results, it's not necessary to count for it
                ++ current_count;
            }
        }
    } else {
        // Step b'. loop for each item in the layer
        for (auto it = dataArray->at(layer).begin() ; it != dataArray->at(layer).end() ; ++ it) {
            // Step c'. create the list for next layer
            QList<int> layer_temp = item;
            layer_temp.push_back(*it);

            // Step d'. call iterator function for next layer
            checkXORRecursively(dataArray, passedCount, layer + 1, layer_temp, gap_count, current_count);
        }
    }
}

// check XOR verification for one xor gap
void checkXORPerMatrix(QList< QList<int> > *source, QList< QList<int> > &result, const int gap_count) {
    // Step a. generate the count list of each group
    QList< QMap<int, int> > passed_count;
    for (int i = 0 ; i < source->size() ; ++ i) {
        QMap<int, int> temp;
        passed_count.push_back(temp);
    }

    // Step b. call the iterator function to calculate for each combination
    QList<int> initial;
    initial.clear();
    int current_count = 0;
    checkXORRecursively(source, passed_count, 0, initial, gap_count, current_count);

    // Step c. frequence check for the sequence numbers, if verified, push to the result
    // the Mcols is removed
    for (int index = 0 ; index < source->size() - 1 ; ++ index) {
        // Step d. result for each group
        QList<int> group;

        // Step e. loop the values in each group
        for (auto it = source->at(index).begin() ; it != source->at(index).end() ; ++ it) {
            // Step f. if verified
            if (passed_count[index][*it] > 0) {
                // Step g. push to the result
                group.push_back(*it);
            }
        }
        // Step h. push the group result to the result
        result.push_back(group);
    }

    // Step i. check the count of each group, if a group is zero, the matrix is corrupted
    for (auto it = result.begin() ; it != result.end() ; ++ it) {
        if (it->size() == 0) {
            result.clear();
            break;
        }
    }

    if (isDEBUG) {
        QString log = "matrix xor checked: [";
        for (auto group = result.begin() ; group != result.end() ; ++ group) {
            log += "[";
            for (auto item : *group) {
                log += QString("%1,").arg(item);
            }
            log += QString("](%1),").arg(group->size());
        }
        log += "],";
        ProcessOperation::logToFile(&log);
    }
}


// check XOR at Mcols, global function
void checkXOR(QList< QList< QList<int> > > *source, QList< QList< QList<int> > > &result, int Lcodeword) {
    // Step a. loop for each matrix
    for (auto it = source->begin() ; it != source->end() ; ++ it) {
        // Step b. check for each matrix
        QList< QList<int> > matrix_result;
        int gap_count = 1 << (Lcodeword + 4);
        checkXORPerMatrix(&(*it), matrix_result, gap_count);

        // Step c. if the count is not zero, push to the result
        if (matrix_result.size() > 0) {
            result.push_back(matrix_result);
        }
    }
}


// eliminate random offset, and change to codeword(integer start from 0)
void eliminateRandomOffsetToCodeword(QList< QList< QList<int> > > &source, int random_seed, int Lcodeword) {
    // Step a. init the random generator, the seed is composed of salt and extracted seed
    qsrand(quint32(random_seed) | quint32(g_randomSalt));
    int symbolMax = 1 << Lcodeword;

    // Step b. loop for each matrix
    for (auto index = 0 ; index < source.size() ; ++ index) {
        // Step c. loop for each group
        for (int group = 0 ; group < source[index].size() ; ++ group) {
            int offset = 4096 + qrand() % 4096;

            // Step d. eliminate for each sequence number
            for (int item = 0 ; item < source[index][group].size() ; ++ item) {
                int data = source[index][group][item];

                // Step e. the random offset is eliminated, and the result starts from 0
                data = (data - 1 + symbolMax - offset % symbolMax) % symbolMax;

                // Step f. write data back
                source[index][group][item] = data;
            }
        }
    }
}


// verify CRC verification section, and change from 3-D to 2-D
void verifyCodewordCRC(QList< QList< QList<int> > > *source, QList< QList<int> > &result, int Lcrc, int Lcodeword) {
    // Step a. calculate the max range of CRC section, for mod function
    int crc_max = 1 << Lcrc;

    // Step b. loop for each matrix
    for (auto matrix = source->begin() ; matrix != source->end() ; ++ matrix) {

        // Step c. loop for each group
        for (auto group = matrix->begin() ; group != matrix->end() ; ++ group) {

            // Step d. verified data list
            QList<int> verified;

            // Step e. loop for each data item
            for (auto item = group->begin() ; item != group->end() ; ++ item) {

                // Step f. seperate the data source and checksum section
                int data = (*item) >> Lcrc;
                int checksum = (*item) % crc_max;

                // Step g. generate binary format of source data
                QString data_binary = QString::number(data, 2);
                while (data_binary.length() < Lcodeword - Lcrc) {
                    data_binary.push_front('0');
                }

                // Step h. append salt and generate full format
                data_binary = QString("%1%2%1").arg(g_hashSalt).arg(data_binary);

                // Step i. calculate CRC(hash) results
                QString crc_result = QCryptographicHash::hash(data_binary.toLocal8Bit(), QCryptographicHash::Md5).toHex();
                QStringRef subResult(&crc_result, 8, sizeof(int) * 2 - 1);

                // Step j. change format from hex string to integer
                int interger_result = subResult.toInt(nullptr, 16) % (1 << Lcrc);

                // Step k. if verification passed, append to the result, CRC section is abandoned
                if (interger_result == checksum) {
                    verified.push_back(data);
                }
            }

            // Step l. append verified group to the end
            result.push_back(verified);
        }
    }
}


// rearrange codewords for period R
void rearrangeForR(QList< QList<int> > *source, QList< QList< QList<int> > > &result, int R) {
    // Step a. collect for R periods
    QList< QList<int> > perR;

    // Step b. loop for each group
    for (int index = 0 ; index < source->size() ; ++ index) {
        // Step c. append list to collection
        perR.push_back(source->at(index));

        // Step d. if reached R + 1, push to result and clear temp
        if ((index + 1) % (R + 1) == 0) {
            result.push_back(perR);
            perR.clear();
        }
    }

    // Step e. if the final R period is terminated, append left sections
    if (perR.size()) {
        result.push_back(perR);
        perR.clear();
    }
}


// optimize the combinations
void optimizeHashCombinations(QList< QList<int> > &combinations, int max_count) {
    int max_layer = combinations.first().size() - 1;

    // Step a. loop until the size is less than max value
    while (combinations.size() > max_count) {
        // Step b. map list for storing items count result
        QList< QMap<int, int> > periodCount;

        // Step c. append map to the list, for each layer
        for (int i = 0 ; i < max_layer ; ++ i) {
            QMap<int, int> temp;
            periodCount.push_back(temp);
        }

        // Step d. loop for each combination, calculate the count value
        for (auto item : combinations) {
            for (int index = 0 ; index < max_layer ; ++ index) {
                int key = item.at(index);

                // Step e. if the key is not in the map, append to it
                if (!periodCount[index].contains(key)) {
                    periodCount[index][key] = 0;
                }

                // Step f. increase the count value
                periodCount[index][key] += 1;
            }
        }

        // Step g. for each combination, abandon the ones with minimun in-degree sum
        QMap<int, QList<int> > in_degree_count;
        for (int index = 0 ; index < combinations.size() ; ++ index) {
            int count = 0;

            // Step k. count the sum of in-degree
            for (int layer = 0 ; layer < max_layer ; ++ layer) {
                count += periodCount[layer][combinations[index][layer]];
            }

            // Step l. push the result to the end
            if (!in_degree_count.contains(count)) {
                QList<int> temp;
                in_degree_count[count] = temp;
            }
            in_degree_count[count].push_back(index);
        }

        // Step m. select the combination with smallest in-degree sum
        QList<int> abandoned_combination;
        abandoned_combination = in_degree_count.first();
        if (abandoned_combination.size() >= (combinations.size() >> 1)) {
            while (abandoned_combination.size() >= (combinations.size() >> 2)) {
                abandoned_combination.pop_back();
            }
        }

        //Step n. select the combinations that are not abandoned
        QSet<int> combination_search;
        combination_search.fromList(abandoned_combination);
        QList< QList<int> > temp_combinations;

        for (int index = 0 ; index < combinations.size() ; ++ index) {
            if (!combination_search.contains(index)) {
                temp_combinations.push_back(combinations[index]);
            }
        }

        // Step o. write selection result back
        combinations.clear();
        combinations = temp_combinations;
    }
}


void verifyHashPerPeriodRecursively(QList< QList<int> > *sourceData, QList< QList<int> > &combinations,
                         int layer, int dataHash, int Lhash, QString stepStr, QList<int> stepList, const int R) {
    // Step a. calculate the max value of hash section, for mod operation
    int max_hash = 1 << Lhash;

    // Step b. if is the R group, means checking hash result
    if (layer == sourceData->size() - 1 && layer == R) {
        // Step c. build the string for hash checksum
        QString source_str = QString("%1%2%1").arg(g_hashSalt).arg(stepStr);

        // Step d. get hash checksum substring
        QString hash_result = QCryptographicHash::hash(source_str.toLocal8Bit(), QCryptographicHash::Sha1).toHex();
        QStringRef subResult(&hash_result, 8, sizeof(int) * 2 - 1);

        // Step e. change format from hex string to binary string
        int interger_result = subResult.toInt(nullptr, 16) % (1 << dataHash);

        // Step f. if calculated result is in the R group, push the combination to the result
        if (sourceData->last().indexOf(interger_result) >= 0) {
            combinations.push_back(stepList);

            if (isDEBUG) {
                QString log = QString("Verified HASH [%1]: \"%2\"").arg(layer).arg(stepStr);
                log += "=" + QString::number(interger_result, 2) + ". datas:[";

                for (auto item : stepList) {
                    log += QString::number(item >> Lhash, 2) + ",";
                }
                log += "].";

                ProcessOperation::logToFile(&log);
            }
        }
    } else {
        // Step b'. if current layer is not the R group, loop items in current layer
        for (auto item : sourceData->at(layer)) {
            // Step c'. prepare the strings for checksum
            QString current_str = stepStr;
            QString data_str = QString::number(item >> Lhash, 2);
            QString item_str = QString::number(item, 2);

            // Step d'. if the length is short than block-length, append zero to the begining
            while (data_str.length() < dataHash - Lhash) {
                data_str.push_front('0');
            }
            while (item_str.length() < dataHash) {
                item_str.push_front('0');
            }

            // Step e'. append data in current group, generate checksum source
            current_str += data_str;

            // Step f'. calculate hash checksum string
            QString source_str = QString("%1%2%1").arg(g_hashSalt).arg(current_str);
            QString hash_result = QCryptographicHash::hash(source_str.toLocal8Bit(), QCryptographicHash::Sha1).toHex();
            QStringRef subResult(&hash_result, 8, sizeof(int) * 2 - 1);

            // Step g'. change format from hex string to binary string
            int interger_result = subResult.toInt(nullptr, 16) % (1 << Lhash);

            // Step h'. judge the calculated result, whether meets the extracted
            if (interger_result == item % max_hash) {
                // Step i'. generate the item list for current combination
                QList<int> current_step = stepList;
                current_step.push_back(item);
                current_str = stepStr + item_str;

                if (layer != sourceData->size() - 1) {
                    // Step j'. call the recursive function, for next layer
                    verifyHashPerPeriodRecursively(sourceData, combinations, layer + 1, dataHash, Lhash, current_str, current_step, R);
                } else {
                    combinations.push_back(current_step);
                }
            }
        }

        // Step k'. if the result exceeds gate value, then remove some
        int max_gate = 1 << (dataHash + 4);
        if (combinations.size() > max_gate) {
            optimizeHashCombinations(combinations, max_gate);
        }
    }
}


void selectFinalForPeriod(QList< QList<int> > *verified_combinations, QList<int> &final_result) {
    // Step a. parameters for calculating the out-degree of all the combinations
    QList< QMap<int, int> > layers_count;
    int layer_max = verified_combinations->first().size();
    for (int i = 0 ; i < layer_max ; ++ i) {
        QMap<int, int> temp;
        layers_count.push_back(temp);
    }

    // Step b. loop for each combination
    for (int index = 0 ; index < verified_combinations->size() ; ++ index) {
        // Step c. loop for each layer in the combinaiton
        for (int layer = 0 ; layer < layer_max ; ++ layer) {
            // Step d. get key of map
            int key = (*verified_combinations)[index][layer];

            // Step e. whether key in the map
            if (!layers_count[layer].contains(key)) {
                layers_count[layer][key] = 0;
            }

            // Step f. increase count value
            layers_count[layer][key] += 1;
        }
    }

    // Step g. calculate weighted sum of degree
    QMap<int, int> sum_of_degrees;
    for (int index = 0 ; index < verified_combinations->size() ; ++ index) {
        int sum = 0;

        // Step h. for a combination, calculate the weighted sum
        for (int layer = 0 ; layer < layer_max ; ++ layer) {
            sum += layers_count[layer][(*verified_combinations)[index][layer]];
        }

        // Step i. push the result into the reversed map
        sum_of_degrees[sum] = index;
    }

    // Step j.select the result with the highest sum
    final_result = verified_combinations->at(sum_of_degrees.last());
}


// verify hash secions global, the crc section is removed
// the final result value, removes the hash section
void verifyHashSection(QList< QList< QList<int> > > *source, QList<int> &result, int Lhash, int dataHash, const int R) {
    // Step a. loop for each R period
    for (int period = 0 ; period < source->size() ; ++ period) {
        QList< QList<int> > verified_combinations;
        QList<int> init_layer;

        // Step b. call verification section
        verifyHashPerPeriodRecursively(&(*source)[period], verified_combinations, 0, dataHash, Lhash, "", init_layer, R);

        // Step c. if final result is empty, append zero
        if (verified_combinations.size() == 0) {
            for (int i = 0 ; i < std::min(source->at(period).size(), R) ; ++ i) {
                result.push_back(0);
            }
        } else {
            // Step d. select the best result from the combinartions
            selectFinalForPeriod(&verified_combinations, init_layer);

            // Step e. push data section to the final list
            for (auto item : init_layer) {
                result.push_back(item >> Lhash);
            }
        }
    }

    if (isDEBUG) {
        QString log = "After Hash Verification : [";

        for (auto item : result) {
            QString binary_string = QString::number(item, 2);
            while (binary_string.length() < dataHash - Lhash) {
                binary_string.push_front('0');
            }
            QString item_string = QString("%1:[%2],").arg(item).arg(binary_string);
            log += item_string;
        }
        log += "]";
        ProcessOperation::logToFile(&log);
    }
}


// assemble covert message
void assembleCovertMessage(QList<int> *codewords, QString &result, int bl_length) {
    // Step a. loop for each data
    for (auto data : *codewords) {
        // Step b. transform from int to binary string
        QString binary_data = QString::number(data, 2);

        // Step c. append zero to the front
        while (binary_data.length() < bl_length) {
            binary_data.push_front('0');
        }

        // Step d. append to result
        result += binary_data;
    }
}


// calculate error bits
void calculateErrorBits(QString *source, QString *target, int &error_bits) {
    // Step a. init error bits
    error_bits = 0;

    // Step b. set compare length to the minimum value
    int min_length = std::min(source->length(), target->length());

    // Step c. loop and compare for each result
    for (int i = 0 ; i < min_length ; ++ i) {
        // Step d. if the value is not
        if (source->at(i) != target->at(i)) {
            ++ error_bits;
        }
    }
}


QString demodulation(int sourceID, int dataID, int Lcodeword, int Lhash, int Lcrc, int Mcols,
                     int R, int sent_bits, QList<int> *dropout_sequence_numbers, int &error_bits) {
    // Step 1. set demodulated result as empty
    QString demodulated = "";

    if (dropout_sequence_numbers->size() == 0) {
        return demodulated;
    }

    // Step 2. seperate the sequence numbers into matrixs
    QList< QList<int> > relative_sequence_numbers;
    std::sort(dropout_sequence_numbers->begin(), dropout_sequence_numbers->end());
    Q_ASSERT(dropout_sequence_numbers->length());
    seperateSequenceNumbersMatrix(dropout_sequence_numbers, relative_sequence_numbers, Lcodeword, Mcols);

    // Step 3. seperate the relative sequence numbers into each group
    QList< QList< int > > symbols_in_each_group;
    Q_ASSERT(relative_sequence_numbers.size());
    seperateMatrixToGroups(&relative_sequence_numbers, symbols_in_each_group, Mcols);
    relative_sequence_numbers.clear();

    // Step 4. rearrange the groups for the matrix
    QList< QList< QList<int> > > symbols_for_xor_verification;
    Q_ASSERT(symbols_in_each_group.size());
    seperateGroupsForXor(&symbols_in_each_group, symbols_for_xor_verification);
    symbols_in_each_group.clear();

    // Step 5. verify XOR results after 2 message groups
    QList< QList< QList<int> > > symbols_group_xored;
    Q_ASSERT(symbols_for_xor_verification.size());
    checkXOR(&symbols_for_xor_verification, symbols_group_xored, Lcodeword);
    symbols_for_xor_verification.clear();

    // Step 6. eliminate random offset, change to codeword
    Q_ASSERT(symbols_group_xored.size());
    eliminateRandomOffsetToCodeword(symbols_group_xored, g_captureSSRC[sourceID], Lcodeword);

    // Step 7. check CRC verification for codewords
    QList< QList<int> > codewords_after_crc;
    Q_ASSERT(symbols_group_xored.size());
    verifyCodewordCRC(&symbols_group_xored, codewords_after_crc, Lcrc, Lcodeword);
    symbols_group_xored.clear();

    // Step 8. regroup codewords for hash verification
    QList< QList< QList<int> > > codewords_group_r;
    Q_ASSERT(codewords_after_crc.size());
    rearrangeForR(&codewords_after_crc, codewords_group_r, R);
    codewords_after_crc.clear();

    // Step 9. verify hash sections, select 1 as final result
    QList<int> verified_codewords;
    Q_ASSERT(codewords_group_r.size());
    verifyHashSection(&codewords_group_r, verified_codewords, Lhash, Lcodeword - Lcrc, R);
    codewords_group_r.clear();

    // Step 10. assemble covert message
    Q_ASSERT(verified_codewords.size());
    assembleCovertMessage(&verified_codewords, demodulated, Lcodeword - Lhash - Lcrc);
    verified_codewords.clear();
    Q_ASSERT(sent_bits <= demodulated.length());

    // Step 11. calculate error bits
    while (sent_bits < demodulated.length()) {
        demodulated.remove(demodulated.length() - 1, 1);
    }
    calculateErrorBits(&g_dataMap[dataID], &demodulated, error_bits);

    return demodulated;
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
    QString sql_cmd = QString("SELECT `LCODEWORD`,`R`,`LHASH`,`LCRC`,`MCOLS` FROM `%1`.`%2` WHERE `ID`='%3'").arg(
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

        log = QString("Table [%1], Lcodeword=%2, R=%3, size(DATA)=%4").arg(this->table).arg(
                    Lcodeword).arg(R).arg(g_dataMap.size());
        ProcessOperation::logToFile(&log);

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
            logQueryError(sql_modulation);
        }

        sql_cmd.clear();
        for (auto id = modulationIDDATA.begin() ; id != modulationIDDATA.end() ; ++id) {
            int error_bits = 0;
            if (isDEBUG) {
                log = QString("Start Demodulating ID [%1]").arg(id.key());
                ProcessOperation::logToFile(&log);
            }

            QString modulated = QString("SELECT `DROPLIST` FROM `%1`.`%2` WHERE `ID`='%3';").arg(
                        this->database->databases["modulation"]).arg(this->table).arg(id.key());

            if (!sql_modulation->exec(modulated)) {
                logQueryError(sql_modulation);
                modulated.clear();
            } else {
                if (sql_modulation->next()) {
                    modulated = sql_modulation->value(0).toString();
                } else {
                    modulated.clear();
                }
            }

            QList<int> dropped_packets;
            praseIntCSVtoList(modulated, dropped_packets);

            dropped_packets += g_sourceMap[modulationIDSOURCE[id.key()]];
            QSet<int> none_repeat_packets = QSet<int>::fromList(dropped_packets);
            dropped_packets = QList<int>::fromSet(none_repeat_packets);

            QString demodulated = demodulation(modulationIDSOURCE[id.key()], id.value(), Lcodeword,
                    Lhash, Lcrc, Mcols, R, modulationIDSentBits[id.key()], &dropped_packets, error_bits);

            if (demodulated.length() != 0) {
                sql_cmd = QString("UPDATE `%1`.`%2` SET `DEMODULATED`='%3',`ERRORBITS`='%4' WHERE `ID`='%5';").arg(
                            this->database->databases["modulation"]).arg(this->table).arg(demodulated).arg(
                            error_bits).arg(id.key());

                if (!sql_modulation->exec(sql_cmd)) {
                    logQueryError(sql_modulation);
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

    log = QString("Thread for [%1] is finished.").arg(this->table);
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

    QString typesCondition;
    QStringList typeList = QString(this->parameter["TYPE"]).split(',');
    for (auto type : typeList) {
        typesCondition += "`TYPE`='" + type + "'";
        if (typeList.indexOf(type) != typeList.length() - 1) {
            typesCondition += " OR ";
        }
    }
    sql_cmd = QString("SELECT `ID`,`LOSTLIST`,`SSRC` FROM `%1`.`%2` WHERE %3;").arg(
                this->parameter["database_source"]).arg(
                this->parameter["tablename_captured"]).arg(typesCondition);
    if (sql_source->exec(sql_cmd)) {
        while (sql_source->next()) {
            int ID = sql_source->value("ID").toInt();
            QString dropList = sql_source->value("LOSTLIST").toString();
            QList<int> droppedPackets;
            ProcessOperation::jsonStringToDropList(&dropList, droppedPackets);
            g_sourceMap[ID] = droppedPackets;
            g_captureSSRC[ID] = sql_source->value("SSRC").toInt();
        }
    } else {
        logQueryError(sql_source);
    }

    processAction(&targetTables);

    //add create view option
    sql_cmd = "CREATE VIEW result_full AS ";
    for (auto table : targetTables) {
        QString temp = QString("(SELECT `PARAID` as `PARAID`,"
                               "`ID` as `ID`,"
                               "`SOURCEID` as `SOURCEID`,"
                               "`DATAID` as `DATAID`,"
                               "`SENTBITS` as `SENTBITS`,"
 //                              "`DEMODULATED` as `DEMODULATED`,"
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
            logQueryError(sql_modulation);
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
