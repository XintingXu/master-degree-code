#include "../include/parameters.h"
#include "../include/processoperation.h"

#include <QCoreApplication>
#include <QDebug>
#include <QThread>
#include <QThreadPool>

int main(int argc, char *argv[]) {
    qDebug() << "Ideal thread Count : " << QThread::idealThreadCount();
    qDebug() << "Current thread pool count : " << QThreadPool::globalInstance()->maxThreadCount();
    QThreadPool::globalInstance()->setExpiryTimeout(-1);
    qDebug() << "Expire Timeout : " << QThreadPool::globalInstance()->expiryTimeout();

    parameters para(nullptr, PARAMETERS_GENERATION);
    database_info_t database_info;
    parameter_t parameter;

    // get configurations of database
    if (para.get_database(&database_info)) {
        qDebug() << "Parameters load OK";
        qDebug() << "Host : " << database_info.host_name;
        qDebug() << "User : " << database_info.user_name;
        qDebug() << "Pass : " << database_info.password;
        for (QMap<QString, QString>::iterator it = database_info.databases.begin();
             it != database_info.databases.end(); ++it) {
            qDebug() << "DataBase[" << it.key() << "] : " << it.value();
        }

        // get parameters of runtime task
        if (para.get_other_parameters(&parameter)) {
            qDebug() << "Other parameters: ";
            for (QMap<QString, QString>::iterator it =parameter.begin(); it != parameter.end(); ++it) {
                qDebug() << "[" << it.key() << "] : " << it.value();
            }
        }

        ProcessOperation op(nullptr, &database_info, &parameter);
        op.process();

        qDebug() << "Finished, Exiting";
        return 0;
    }

    qDebug() << "Finished, Exiting";
    return -1;
}
