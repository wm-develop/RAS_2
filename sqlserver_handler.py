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
                CONVERT(VARCHAR(16), btime, 120) AS btime_formatted,
                CONVERT(VARCHAR(16), etime, 120) AS etime_formatted
            FROM 
                wds.sd_dsp_scheme_info
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

        conn = self._get_connect()
        cursor = conn.cursor()
        query = """
            SELECT 
                ssd.DATAVALUE,
                ssd.OBJDESC
            FROM 
                wds.SD_DSP_SCHEME_INFO ssi
            JOIN 
                wds.SD_DSP_SCHEME_DATA ssd
            ON 
                ssi.SCHEME_NAME = ssd.SCHEME_NAME
            WHERE 
                ssi.SCHEME_NAME = %s
                AND ssi.BTIME >= %s
                AND ssi.ETIME <= %s
                AND ssd.DATAKEY = 'RSVR_OUTFLOW_PRO'
            """
        # 执行SQL查询
        cursor.execute(query, (scheme_name, ymdhm_start, ymdhm_end))
        # 获取查询结果
        results = cursor.fetchall()

        # 创建一个字典来存储流量过程
        flow_data = {
            "磨子潭": None,
            "白莲崖": None,
            "佛子岭": None,
            "响洪甸": None
        }

        # 解析查询结果
        for datavalue, objdesc in results:
            flow_values = list(map(str, datavalue.split(';')))
            flow_values = list(map(float, flow_values[:-1]))
            if "MZL" in objdesc or "MZT" in objdesc:
                flow_data["磨子潭"] = flow_values
            elif "BLY" in objdesc:
                flow_data["白莲崖"] = flow_values
            elif "FZL" in objdesc:
                flow_data["佛子岭"] = flow_values
            elif "XHD" in objdesc:
                flow_data["响洪甸"] = flow_values

        # 判断flow_data中各个一维数组中所包含的元素数量是否相同
        lengths = [len(arr) for arr in flow_data.values()]
        if not lengths:
            raise NoArraysInDictionaryError()
        else:
            if not all(length == lengths[0] for length in lengths):
                raise ArrayLengthsMismatchError()

        # 将结果转换为二维数组
        ndarray = np.array([flow_data["磨子潭"], flow_data["白莲崖"], flow_data["佛子岭"], flow_data["响洪甸"]], dtype=float).T
        cursor.close()
        conn.close()
        return ndarray


# 下面是用于临时测试的代码
if __name__ == '__main__':
    server = '2603711m5f.zicp.vip'
    port = 10281
    user_name = 'wdsfzl'
    password = 'wds#WDS9200'
    sqlserver_handler = SQLServerHandler(server, port, user_name, password, "wdsfzl")
    # print(sqlserver_handler.q_from_table("2024-10-27 01:00:00制作方案仿真", "2024-10-27 01:00", "2024-10-30 00:00"))
    print(sqlserver_handler.get_start_end_time("2024-10-27 01:00:00制作方案仿真"))
