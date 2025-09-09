# -*- coding: UTF-8 -*-
import numpy as np
import pymssql
from datetime import datetime
from logger import logger


class NoArraysInDictionaryError(Exception):
    def __init__(self, message="无法查询到任何水库的出库流量过程"):
        self.message = message
        super().__init__(self.message)


class ArrayLengthsMismatchError(Exception):
    def __init__(self, message="数据库中各水库出库流量的时间步数不完全相同"):
        self.message = message
        super().__init__(self.message)


class NegativeFlowError(Exception):
    def __init__(self, message="水库出库流量序列中存在负值"):
        self.message = message
        super().__init__(self.message)


class CalInfoDataError(Exception):
    def __init__(self, message="响洪甸水库cal_info数据为空或数量与模拟时长不符"):
        self.message = message
        super().__init__(self.message)


class SQLServerHandler:
    def __init__(self, server, port, username, password, database):
        """
        连接数据库
        :param server: SQL Server数据库的地址
        :param username: 用户名
        :param password: 密码
        :param database: 数据库名
        """
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.database = database

    def _get_connect(self):
        return pymssql.connect(server=self.server, port=self.port, user=self.username, password=self.password, database=self.database)

    def get_start_end_time(self, scheme_name):
        conn = self._get_connect()
        cursor = conn.cursor()
        # 查询 btime 和 etime
        query = '''
            SELECT 
                CONVERT(VARCHAR(16), begin_time, 120) AS btime_formatted,
                CONVERT(VARCHAR(16), end_time, 120) AS etime_formatted
            FROM 
                wds.hps_dsp_result_scheme
            WHERE 
                scheme_name = %s
        '''
        cursor.execute(query, (scheme_name,))
        result = cursor.fetchone()

        # 检查是否获取到了结果
        if result:
            btime_formatted, etime_formatted = result
            cursor.close()
            conn.close()
            return btime_formatted, etime_formatted
        else:
            cursor.close()
            conn.close()
            return None, None

    def q_from_table(self, scheme_name, ymdhm_start, ymdhm_end):
        ymdhm_start = datetime.strptime(ymdhm_start, "%Y-%m-%d %H:%M")
        ymdhm_end = datetime.strptime(ymdhm_end, "%Y-%m-%d %H:%M")

        # 计算模拟时长（小时数）
        simulation_hours = int((ymdhm_end - ymdhm_start).total_seconds() / 3600) + 1

        conn = self._get_connect()
        cursor = conn.cursor()

        try:
            # 存储各水库的流量数据
            reservoir_flows = {}

            # 水库ID映射：1039-白莲崖, 1041-佛子岭, 1043-磨子潭
            reservoir_ids = [1043, 1039, 1041]  # 按照返回数组的列顺序：磨子潭、白莲崖、佛子岭
            reservoir_names = ['磨子潭', '白莲崖', '佛子岭']

            # 查询每个水库的出库流量数据
            for i, reservoir_id in enumerate(reservoir_ids):
                query = '''
                        SELECT data_time, (gen_flow + other_outflow + disp_flow) AS total_outflow
                        FROM wds.hps_dsp_result_rsvr_dat
                        WHERE scheme_name = %s
                          AND reservoir_id = %s
                          AND data_time >= %s
                          AND data_time <= %s
                        ORDER BY data_time
                '''
                cursor.execute(query, (scheme_name, reservoir_id, ymdhm_start, ymdhm_end))
                results = cursor.fetchall()

                if not results:
                    raise NoArraysInDictionaryError(f"无法查询到{reservoir_names[i]}水库的出库流量过程")

                # 提取流量数据
                flows = [float(row[1]) for row in results]

                # 检查是否存在负值
                if any(flow < 0 for flow in flows):
                    raise NegativeFlowError(f"{reservoir_names[i]}水库出库流量序列中存在负值")

                reservoir_flows[reservoir_id] = flows

            # 查询响洪甸水库的cal_info数据
            query_cal_info = '''
                             SELECT cal_info
                             FROM wds.hps_dsp_result_scheme
                             WHERE scheme_name = %s \
                             '''
            cursor.execute(query_cal_info, (scheme_name,))
            cal_info_result = cursor.fetchone()

            if not cal_info_result or not cal_info_result[0]:
                raise CalInfoDataError("响洪甸水库cal_info数据为空")

            cal_info_str = cal_info_result[0].strip()
            if not cal_info_str:
                raise CalInfoDataError("响洪甸水库cal_info数据为空")

            # 解析cal_info数据
            try:
                xianghongdian_flows = [float(x.strip()) for x in cal_info_str.split(',') if x.strip()]
            except ValueError:
                raise CalInfoDataError("响洪甸水库cal_info数据格式错误")

            # 检查cal_info数据长度是否与模拟时长相符
            if len(xianghongdian_flows) != simulation_hours:
                raise CalInfoDataError(
                    f"响洪甸水库cal_info数据数量({len(xianghongdian_flows)})与模拟时长({simulation_hours}小时)不符")

            # 检查响洪甸流量是否存在负值
            if any(flow < 0 for flow in xianghongdian_flows):
                raise NegativeFlowError("响洪甸水库出库流量序列中存在负值")

            # 检查各水库数据长度是否一致
            expected_length = simulation_hours
            for reservoir_id, flows in reservoir_flows.items():
                if len(flows) != expected_length:
                    reservoir_name = reservoir_names[reservoir_ids.index(reservoir_id)]
                    raise ArrayLengthsMismatchError(
                        f"{reservoir_name}水库流量数据长度({len(flows)})与预期长度({expected_length})不符")

            # 构建返回的二维数组 (n行4列)
            # 列顺序：磨子潭(0)、白莲崖(1)、佛子岭(2)、响洪甸(3)
            array = np.zeros((simulation_hours, 4))

            # 填充数据
            array[:, 0] = reservoir_flows[1043]  # 磨子潭
            array[:, 1] = reservoir_flows[1039]  # 白莲崖
            array[:, 2] = reservoir_flows[1041]  # 佛子岭
            array[:, 3] = xianghongdian_flows  # 响洪甸

            return array

        except Exception as e:
            raise e

        finally:
            cursor.close()
            conn.close()


# 下面是用于临时测试的代码
if __name__ == '__main__':
    # server = '2603711m5f.zicp.vip'
    # port = 10281
    # user_name = 'wdsfzl'
    # password = 'wds#WDS9200'
    # sqlserver_handler = SQLServerHandler(server, port, user_name, password, "wdsfzl")
    # # print(sqlserver_handler.q_from_table("2024-10-27 01:00:00制作方案仿真", "2024-10-27 01:00", "2024-10-30 00:00"))
    # print(sqlserver_handler.get_start_end_time("2024-10-27 01:00:00制作方案仿真"))

    server = '10.34.202.189'
    port = 1433
    user_name = 'wds'
    password = 'nari2008'
    sqlserver_handler = SQLServerHandler(server, port, user_name, password, "wds")
    ymdhm_start, ymdhm_end = sqlserver_handler.get_start_end_time("方案制作时间:2025-09-08 16:11:41仿真")
    print(ymdhm_start, ymdhm_end)
    print(sqlserver_handler.q_from_table("方案制作时间:2025-09-08 16:11:41仿真", ymdhm_start, ymdhm_end))
