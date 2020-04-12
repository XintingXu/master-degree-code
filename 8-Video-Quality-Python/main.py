"""
Video Quality Metrics
Copyright (c) 2014 Alex Izvorski <aizvorski@gmail.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import numpy
import re
import sys
import scipy.misc

import psnr
import niqe

import rtp2h264
import pymysql
import os
import cv2


def img_greyscale(img):
    return 0.299 * img[:, :, 0] + 0.587 * img[:, :, 1] + 0.114 * img[:, :, 2]


def evaluation_h264(h264_filename: str):
    temp_niqe = list()

    video = cv2.VideoCapture(h264_filename)
    if video.isOpened():
        frame_count = 0
        while True:
            flag, frame = video.read()
            if not flag:
                break
            frame_count = frame_count + 1
            # cv2.imwrite("./demo/{0}.jpg".format(frame_count), frame)
            array = numpy.array(img_greyscale(frame)).astype(numpy.float)/255.0
            #print("NIQE = {0}".format(niqe.niqe(array)))

            niqe_result = 0.0
            try:
                niqe_result = niqe.niqe(array)
            except:
                print("NIQE Error at frame [{0}]".format(frame_count))
            else:
                temp_niqe.append(niqe_result)

    return temp_niqe


def list_to_csv(source_list: list):
    result = ""
    for item in source_list:
        result += "{0},".format(item)
    result = result[:len(result) - 1]
    return result


if __name__ == '__main__':
    print("Start Evaluating Video Quality")

    sourceIDs = [16, 17]

    db_evaluation = pymysql.connect("192.168.0.108", "xuxinting", "xuxinting", "evaluation", charset='utf8mb4')
    cursor_evaluation = db_evaluation.cursor()

    sql = "TRUNCATE `evaluation`.`videoquality`;"
    cursor_evaluation.execute(sql)

    for source in sourceIDs:
        print("Start source[{0}]".format(source))

        reference_cap_file = "{0}.pcap".format(source)
        reference_h264_file = "{0}.264".format(source)
        reference_packet_list = rtp2h264.load_pcap_file(reference_cap_file)
        rtp2h264.write_into_file(reference_h264_file, reference_packet_list, [])

        niqe_str = list_to_csv(evaluation_h264(reference_h264_file))
        sql = "INSERT INTO `evaluation`.`videoquality`(`SOURCEID`,`PARAID`,`NIQE`) VALUES('{0}','0','{1}')".format(
            source, niqe_str
        )
        cursor_evaluation.execute(sql)
        db_evaluation.commit()

        sql = "SELECT `ID` FROM `source`.`parameters`;"
        cursor_evaluation.execute(sql)
        cursor_result = cursor_evaluation.fetchall()

        parameterIDs = list()
        for item in cursor_result:
            parameterIDs.append(item[0])

        for para in parameterIDs:
            print("Start parameter [{0}]".format(para))

            sql = "SELECT `DROPLIST` FROM `modulation`.`{0}` WHERE `SOURCEID`='{1}' LIMIT 1;".format(para, source)
            cursor_evaluation.execute(sql)
            drop_list = str(cursor_evaluation.fetchone()[0]).split(',')

            drop_list_int = list()
            for seq in drop_list:
                drop_list_int.append(int(seq))

            current_file_name = "{0}.{1}.264".format(source, para)
            rtp2h264.write_into_file(current_file_name, reference_packet_list, drop_list_int)

            niqe_str = list_to_csv(evaluation_h264(current_file_name))
            sql = "INSERT INTO `evaluation`.`videoquality`(`SOURCEID`,`PARAID`,`NIQE`) VALUES('{0}','{1}','{2}')".format(
                source, para, niqe_str
            )
            cursor_evaluation.execute(sql)
            db_evaluation.commit()

            os.remove(current_file_name)

        print("finish source[{0}]".format(source))
        reference_packet_list.clear()
        os.remove(reference_h264_file)

    cursor_evaluation.close()
    db_evaluation.commit()
    db_evaluation.close()
    print("Finish Evaluating")
