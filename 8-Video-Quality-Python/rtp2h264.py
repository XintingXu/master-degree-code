#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import dpkt
import copy
import os

# 采用列表的形式，依次添加每个数据包的信息
# 对于单个数据包来说，采用dict字典按照键值对进行存储
# RTP中的部分特征
# 'RTP_timestamp': RTP中的时间戳信息，长度为4字节，转换为int整型进行识别
# 'RTP_sequence_number': RTP中的数据包序号，长度为2字节，转换为int整型进行识别
# 'RTP_mark': RTP中的mark标记，长度为1位，转换为int整型进行识别
# fu_identifier对于单包和分片包来说，都存在
# 'fu_identifier_nri': H.264中，fu_identifier头中的nri字段，长度为2位，简化为int整型进行识别
# 'fu_identifier_type': H.264中，fu_identifier头中的type字段，长度为5位，简化为int整型进行识别
# 'fu_identifier': H.264中，fu_identifier头字段，长度为1个字节，按照原始的bytes直接存储，便于后续的拼接
# fu_header只存在于分片的数据包(fu_identifier_type=28)中
# 'fu_header_start_bit': H.264中，fu_header头中的start bit字段，长度为1位，简化为int整型进行识别
# 'fu_header_end_bit': H.264中，fu_header头中的end bit字段，长度为1位，简化为int整型进行识别
# 'fu_header_forbidden_bit': H.264中，fu_header头中的forbidden bit字段，长度为1位，简化为int整型进行识别
# 'fu_header_nalu_type': H.264中，fu_header头中的Nal_unit_type字段，长度为5位，简化为int整型进行识别
# 'fu_header': H.264中，fu_identifier头字段，长度为1个字节，按照原始的bytes直接存储，便于后续的拼接
# 'fu_payload': H.264中，去掉fu_identifier头和fu_header头之后的负载数据，按照原始的bytes直接存储，便于之后的拼接


def write_into_file(file_name: str, rtp_packet_info: list, dropout_list: list):
    if os.path.exists(file_name):
        os.remove(file_name)
    dropout_list = list(dropout_list)
    if len(rtp_packet_info) == 0:
        print("Packet analyzer returned 0 results.")
    else:
        h264_file = open(file_name, 'wb')
        for item in rtp_packet_info:
            item = dict(item)

            if len(item) == 0 or item['RTP_sequence_number'] in dropout_list:
                pass
            else:
                # 属于单包的情况
                if 1 <= item['fu_identifier_type'] <= 23:
                    h264_file.write(b"\x00\x00\x00\x01")
                    h264_file.write(item['fu_identifier'])
                    h264_file.write(item['fu_payload'])

                # 属于FU-A的分片数据包情况
                elif item['fu_identifier_type'] == 28:
                    # 如果带有mark标记
                    if item['RTP_mark']:
                        h264_file.write(item['fu_payload'])

                    # 如果是中间的数据包
                    else:
                        # 如果是分片的第一个包
                        if item['fu_header_start_bit']:
                            h264_file.write(b"\x00\x00\x00\x01")
                            fb = int(item['fu_header_forbidden_bit']) << 7
                            nri = int(item['fu_identifier_nri']) << 5
                            nalu_t = int(item['fu_header_nalu_type'])
                            extended_header = fb | nri | nalu_t
                            extended_header = extended_header.to_bytes(1, byteorder='big', signed=False)
                            h264_file.write(extended_header)
                            h264_file.write(item['fu_payload'])
                        # 如果是分片中间的包
                        else:
                            h264_file.write(item['fu_payload'])

                # 因为其它类型在实验中未出现，不进行考虑
                else:
                    pass
        h264_file.close()


def load_pcap_file(pcap_file_name: str):
    file = open(pcap_file_name, 'rb')
    reader = dpkt.pcap.Reader(file)
    rtp_packet_info = list()

    for ts, pkt in reader:
        # print("ts: {0}".format(ts))

        pkt = bytearray(pkt)[16:]
        # print("pkt: {0}".format(pkt))
        ip = dpkt.ip6.IP6(bytes(pkt))

        udp = dpkt.udp.UDP(bytes(ip.data))
        rtp = dpkt.rtp.RTP(bytes(udp.data))
        rtp_timestamp = rtp.ts
        if rtp.m:
            # RFC5285，拓展头的规定
            # 判断是否存在0xbede的2字节固定标识，如果存在，则意味着有拓展头部分
            rtp_data = bytearray(rtp.data)
            first_two_bytes = bytes(rtp_data[:2])

            # 存在拓展头
            if first_two_bytes == b'\xbe\xde':
                # 去掉0xbede固定头
                rtp_data = rtp_data[2:]
                extension_length = bytes(rtp_data[:2])
                # 移除拓展头长度占用的2字节
                rtp_data = rtp_data[2:]
                # 获取拓展头的长度信息
                extension_length = int.from_bytes(extension_length, signed=False, byteorder='big')
                rtp_data = rtp_data[((extension_length + 1) // 2) * 4:]

            # 不存在拓展头
            else:
                pass

            # print("RTP Sequence number: {0}, mark, payload: {1}".format(rtp.seq, bytes(rtp_data)))
        else:
            rtp_data = rtp.data
            # print("RTP Sequence number: {0}, payload: {1}".format(rtp.seq, rtp.data))
        # print("rtp_data: {0}".format(bytes(rtp_data)))

        fu_identifier = bytearray(rtp_data)[:1]
        fu_identifier = bytes(fu_identifier)
        # print("fu_identifier: {0}".format(fu_identifier))
        fu_identifier_int = int.from_bytes(fu_identifier, signed=False, byteorder='big')
        fu_identifier_nri = (fu_identifier_int >> 5) % pow(2, 2)
        fu_identifier_type = fu_identifier_int % pow(2, 5)
        fu_payload = bytes()

        # fu_header只存在于分片的数据包(fu_identifier_type=28)中
        # 'fu_header_start_bit': H.264中，fu_header头中的start bit字段，长度为1位，简化为int整型进行识别
        # 'fu_header_end_bit': H.264中，fu_header头中的end bit字段，长度为1位，简化为int整型进行识别
        # 'fu_header_forbidden_bit': H.264中，fu_header头中的forbidden bit字段，长度为1位，简化为int整型进行识别
        # 'fu_header_nalu_type': H.264中，fu_header头中的Nal_unit_type字段，长度为5位，简化为int整型进行识别
        # 'fu_header': H.264中，fu_identifier头字段，长度为1个字节，按照原始的bytes直接存储，便于后续的拼接
        # 'fu_payload': H.264中，去掉fu_identifier头和fu_header头之后的负载数据，按照原始的bytes直接存储，便于之后的拼接
        packet_info = dict()
        packet_info['RTP_timestamp'] = int(rtp_timestamp)
        packet_info['RTP_sequence_number'] = int(rtp.seq)
        packet_info['RTP_mark'] = int(rtp.m)

        packet_info['fu_identifier_nri'] = fu_identifier_nri
        packet_info['fu_identifier_type'] = fu_identifier_type
        packet_info['fu_identifier'] = fu_identifier

        # 因为在捕获的数据中，只有FU-A和PPS、SPS几种，只有FU-A采用的是分片的多包模式，其余均为单包，可以简化处理
        # 如果是FU-A类型
        if fu_identifier_type == 28:
            fu_header = bytes(bytearray(rtp_data)[1:2])
            fu_header_int = int.from_bytes(fu_header, signed=False, byteorder='big')
            fu_header_start_bit = fu_header_int >> 7
            fu_header_end_bit = (fu_header_int >> 6) % 2
            fu_header_forbidden_bit = (fu_header_int >> 5) % 2
            fu_header_nalu_type = fu_header_int % pow(2, 5)
            fu_payload = bytes(bytearray(rtp_data)[2:])

            packet_info['fu_header_start_bit'] = fu_header_start_bit
            packet_info['fu_header_end_bit'] = fu_header_end_bit
            packet_info['fu_header_forbidden_bit'] = fu_header_forbidden_bit
            packet_info['fu_header_nalu_type'] = fu_header_nalu_type
            packet_info['fu_header'] = fu_header
            packet_info['fu_payload'] = fu_payload

            rtp_packet_info.append(copy.deepcopy(packet_info))

            # 如果是单包类型
        elif 1 <= fu_identifier_type <= 23:
            fu_payload = bytes(bytearray(rtp_data)[1:])
            packet_info['fu_payload'] = fu_payload

            rtp_packet_info.append(copy.deepcopy(packet_info))

        # 其它类型，不进行处理
        else:
            pass

        packet_info.clear()
    file.close()

    return rtp_packet_info


if __name__ == '__main__':
    prefix_path = './demo/'

    for i in range(16, 18):
        pcap_file_name = prefix_path + '{0}.pcap'.format(i)
        rtp_packet_info = load_pcap_file(pcap_file_name)
        write_into_file(pcap_file_name.replace('.pcap', '.264', 1), rtp_packet_info, [])
        rtp_packet_info.clear()
        print("Done @{0}".format(i))
