# -*- coding: UTF-8 -*-
import os
from subprocess import Popen, PIPE, STDOUT

import logging
logger = logging.getLogger(__name__)

class RASHandler:
    def __init__(self, qc_list: list):
        """
        构造方法
        :param qc_list:  指定时段的某断面流量列表，注意不能超过6位（若有小数点则小数点也算一位）
        """
        self.qc_list = qc_list

    def modify_u01(self, filepath: str, output_path: str):
        """
        将指定时段的的断面流量列表写入u01文件中
        方法假定只有一个上游断面，没有支流汇入口，如有多个支流汇入，需要额外搜索Boundary Location=一行找到断面的名称，未来做祊河时需要实现
        :param output_path: 输出文件的路径
        :param filepath: u01文件的路径
        :return: 无返回值
        """

        # 总体思路：查找Flow Hydrograph和Stage Hydrograph TW Check所在的行数，两者中间的内容就是要修改的时间序列
        # 先将原文件FH以上的内容原封不动地写入到新文件，之后将新的时间序列写入新文件，最后将SHTC之后的内容写入

        # 查找Flow Hydrograph所在的行数
        fh_line_index = self.__str_search(filepath, 'Flow Hydrograph=')[0]
        # 查找Stage Hydrograph TW Check所在的行数
        shtc_line_index = self.__str_search(
            filepath, 'Stage Hydrograph TW Check=')[0]

        with open(filepath, mode='r', encoding='utf-8') as f:
            file_lines = f.readlines()
            # 读取FH以上的所有内容（不包含FH这一行），准备后续复制一份到新文件中
            fh_up = file_lines[:fh_line_index]
            # 读取SHTC以下的所有内容（包含SHTC这一行），准备后续复制一份到新文件中
            shtc_down = file_lines[shtc_line_index:]

        with open(output_path, mode='w', encoding='utf-8') as f:
            # 先将原文件除时间序列之外的内容原封不动地抄写过来
            f.writelines(fh_up)  # 此时游标位于FH行的开头
            # 把FH行写到文件中
            # qc_len为一字符串类型的变量，代表qc_list（新时间序列）中包含的元素数量
            qc_len = f"{len(self.qc_list)}"
            f.writelines(f"Flow Hydrograph= {qc_len} \n")

            # 把时间序列写到文件中
            qc_10_str = ''
            qc_str_list = list()
            for i, qc in enumerate(self.qc_list):
                # 每10个数据一组构建一个字符串，末尾需要加换行符
                space = self.__judge_qc(str(qc))
                qc_10_str = qc_10_str + space + str(qc)
                if (i + 1) % 10 == 0:
                    qc_10_str += '\n'
                    qc_str_list.append(qc_10_str)
                    qc_10_str = ''
                elif i == len(self.qc_list) - 1:  # 如果是最后一次循环且时间序列数不为10的倍数
                    qc_10_str += '\n'  # 在末尾增加一个换行符

            f.writelines(qc_str_list)  # 将前n组时间序列数据写入文件
            f.writelines(qc_10_str)  # 将多余的时间序列数据写入文件

            f.writelines(shtc_down)  # 将SHTC行及后续内容写到文件中

    def __judge_qc(self, qc: str):
        """
        判断qc_list中的每个qc的字符数，相邻两个qc的组合可分为以下几种情况：
        1+1,1+2,1+3,...,1+6；
        2+1,2+2,2+3,...,2+6；
        ...
        6+1,6+2,6+3,...,6+6.
        相邻两qc之间需要加的空格数根据组合的不同而有所不同
        经实验发现，相邻两qc之间所需的空格数仅与第二个qc有关而与第一个qc无关，所需的空格数为：
        x+1(7),x+2(6),x+3(5),x+4(4),x+5(3),x+6(2)
        其中，x为第一个qc的位数
        此方法为私有方法，只能在类内被调用
        :param qc: 某时刻的断面流量
        :return: 返回需要增加的空格数
        """
        if len(qc) == 1:
            return '       '
        elif len(qc) == 2:
            return '      '
        elif len(qc) == 3:
            return '     '
        elif len(qc) == 4:
            return '    '
        elif len(qc) == 5:
            return '   '
        elif len(qc) == 6:
            return '  '

    def __str_search(self, filepath, string):
        """
        查找给定字符串在文件中的位置，list_line列表存放字符串所在的行数
        :param filepath:
        :param string:
        :return:
        """
        count = 0
        list_line = []
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f.readlines():
                if string in line:
                    list_line.append(count)
                count += 1
        return list_line

    def modify_p01(self, filepath, output_path, time_start='02MAY2023,0000', time_end='03MAY2023,0700', ci='20SEC',
                   oi_ii_mi='10MIN'):
        """
        将模拟时段、时间步长信息写入p01文件中
        需要改第4行（模拟时间），第26-29行（输出时间步长）
        :param output_path: 输出文件的路径
        :param time_start: 起始时间，要求为字符串类型，格式为DDMMMYYYY,HHMM
        :param time_end: 结束时间，要求为字符串类型，格式为DDMMMYYYY,HHMM
        :param ci: Computation Interval，要求为字符串类型，只能为以下特定的值：
        ci_allow_list = ['0.1SEC', '0.2SEC', '0.3SEC', '0.4SEC', '0.5SEC', '1SEC', '2SEC', '3SEC', '4SEC', '5SEC', '6SEC', '10SEC', '12SEC', '15SEC', '20SEC', '30SEC', '1MIN', '2MIN', '3MIN', '4MIN', '5MIN', '6MIN', '10MIN', '12MIN', '20MIN']
        :param oi_ii_mi: Hydrograph Output Interval && Detailed Output Interval && Mapping Output Interval，单位为分钟，默认10MIN
        要求oi, ii, mi必须相同，所以用一个变量代表。上述三个变量只能为以下特定的值：
        oi_ii_mi_allow_list = ['10MIN', '12MIN', '15MIN', '20MIN', '30MIN', '1HOUR', '2HOUR', '3HOUR', '4HOUR']
        :param filepath: p01文件的路径
        :return: 无返回值
        """
        # 查找模拟时间所在的行数
        sd_line_index = self.__str_search(filepath, 'Simulation Date=')[0]
        # 查找Computation Interval所在的行数
        ci_line_index = self.__str_search(filepath, 'Computation Interval=')[0]
        # 查找Computation Time Step Use Courant所在的行数
        ctsuc_line_index = self.__str_search(
            filepath, 'Computation Time Step Use Courant=')[0]

        with open(filepath, mode='r', encoding='utf-8') as f:
            file_lines = f.readlines()
            # 读取SD以上的所有内容（不包含SD这一行），准备后续复制一份到新文件中
            sd_up = file_lines[:sd_line_index]
            # 读取SD以下到CI以上的所有内容（不包含CI这一行），准备后续复制一份到新文件中
            sd_ci = file_lines[sd_line_index + 1:ci_line_index]
            # 读取MI以下的所有内容（不包含MI这一行），准备后续复制一份到新文件中
            mi_down = file_lines[ctsuc_line_index:]

        ci_allow_list = ['0.1SEC', '0.2SEC', '0.3SEC', '0.4SEC', '0.5SEC', '1SEC', '2SEC', '3SEC', '4SEC', '5SEC',
                         '6SEC', '10SEC', '12SEC', '15SEC', '20SEC', '30SEC', '1MIN', '2MIN', '3MIN', '4MIN', '5MIN',
                         '6MIN', '10MIN', '12MIN', '15MIN', '20MIN']
        oi_ii_mi_allow_list = ['10MIN', '12MIN', '15MIN',
                               '20MIN', '30MIN', '1HOUR', '2HOUR', '3HOUR', '4HOUR']

        with open(output_path, mode='w', encoding='utf-8') as f:
            f.writelines(sd_up)  # 此时游标位于SD行的开头
            # 把SD行写到文件中，SD行的参考格式为：Simulation Date=01MAY2023,0000,02MAY2023,0500
            sd_line = f"Simulation Date={time_start},{time_end}"
            f.writelines(f"{sd_line}\n")

            # 把SD以下到CI以上的所有内容写到文件中
            f.writelines(sd_ci)

            # 把四个时间步长信息行写入文件中
            ci_line = f"Computation Interval={ci}"
            oi_line = f"Output Interval={oi_ii_mi}"
            ii_line = f"Instantaneous Interval={oi_ii_mi}"
            mi_line = f"Mapping Interval={oi_ii_mi}"
            f.writelines(f"{ci_line}\n")
            f.writelines(f"{oi_line}\n")
            f.writelines(f"{ii_line}\n")
            f.writelines(f"{mi_line}\n")

            # 把MI以下的所有内容写入文件中
            f.writelines(mi_down)

    def modify_b01(self, filepath, output_path, time_start='02May2023 0000', time_end='03May2023 0700', ci='1MIN',
                   oi_ii_mi='10MIN'):
        """
        修改b01文件供ras Linux计算时调用
        目前只实现了修改Start Date和End Date的功能，修改时间步长的功能暂未实现
        :param filepath: b01文件的路径
        :param output_path: 输出文件的路径
        :param time_start: 起始时间，要求为字符串类型，格式为DDMmmYYYY HHMM
        :param time_end: 结束时间，要求为字符串类型，格式为DDMmmYYYY HHMM
        :param ci: Computation Interval，要求为字符串类型，只能为以下特定的值：
        ci_allow_list = ['0.1SEC', '0.2SEC', '0.3SEC', '0.4SEC', '0.5SEC', '1SEC', '2SEC', '3SEC', '4SEC', '5SEC', '6SEC', '10SEC', '12SEC', '15SEC', '20SEC', '30SEC', '1MIN', '2MIN', '3MIN', '4MIN', '5MIN', '6MIN', '10MIN', '12MIN', '20MIN']
        :param oi_ii_mi: Hydrograph Output Interval && Detailed Output Interval && Mapping Output Interval，单位为分钟，默认10MIN
        要求oi, ii, mi必须相同，所以用一个变量代表。上述三个变量只能为以下特定的值：
        oi_ii_mi_allow_list = ['10MIN', '12MIN', '15MIN', '20MIN', '30MIN', '1HOUR', '2HOUR', '3HOUR', '4HOUR']
        :return: 无返回值
        """
        # 查找模拟时间所在的行数
        sd_line_index = self.__str_search(filepath, '  Start Date/Time')[0]
        # 查找初始条件所在的行数
        ic_line_index = self.__str_search(
            filepath, 'Initial Conditions (use restart file?)')[0]

        with open(filepath, mode='r', encoding='utf-8') as f:
            file_lines = f.readlines()
            # 读取SD以上的所有内容（不包含SD这一行），准备后续复制一份到新文件中
            sd_up = file_lines[:sd_line_index]
            logger.info(sd_up)
            # 读取IC以下的所有内容（包含IC这一行），准备后续复制一份到新文件中
            ic_down = file_lines[ic_line_index:]
            logger.info(ic_down)

        with open(output_path, mode='w', encoding='utf-8') as f:
            f.writelines(sd_up)

            # 把SD和ED行写到文件中，参考格式为：  Start Date/Time       = 26Mar2023 0900
            sd_line = f"  Start Date/Time       = {time_start}"
            ed_line = f"  End Date/Time         = {time_end}"
            f.writelines(f"{sd_line}\n")
            f.writelines(f"{ed_line}\n")

            # 把MI以下的所有内容写入文件中
            f.writelines(ic_down)

    def run_model(self, filepath):
        """
        运行模型
        :param filepath: 可执行文件路径
        :type filepath: str
        :return: 进程返回值, 0 成功, 非 0 失败
        :rtype: int
        """
        path, name = os.path.split(filepath)

        # 创建子进程
        proc: Popen = Popen(args=["bash", name],
                            stdout=PIPE, stderr=STDOUT, cwd=path)

        # 获取进程输出
        for line in proc.stdout:
            logger.info(line.decode().rstrip())

        # 等待子进程结束
        return proc.wait()


# 下面是用于临时测试的代码
if __name__ == '__main__':
    qc_list = [0, 44.1, 43.182, 38.437, 34.07, 1.2, 60, 800, 13.2, 0, 0]
    # qc_list = [i for i in range(100)]
    ras_handler = RASHandler(qc_list)
    filepath = "TestData/syh.u01"
    output_path = 'TestData/syh_test.u01'
    # ras_handler.modify_u01(filepath, output_path)
    filepath = 'TestData/syh.p01'
    output_path = 'TestData/syh_test.p01'
    # ras_handler.modify_p01(filepath, output_path)
    ras_handler.modify_b01(r"E:\rastestdata\ras_syh_01_by61_shiji\syh.b01",
                           r"E:\rastestdata\ras_syh_01_by61_shiji\syh.tmp.b01")
