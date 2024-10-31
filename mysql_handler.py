# -*- coding: UTF-8 -*-
import datetime

import numpy as np
import pymysql

from logger import logger


class MySQLHandler:

    def __init__(self, host, port, user, password, database, charset='utf8'):
        self.host: str = host
        self.port: int = port
        self.user: str = user
        self.password: str = password
        self.database: str = database
        self.charset: str = charset

    def _get_connect(self):
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def _batch_insert(self, sql: str, params: list, step: int = 10000) -> None:
        try:
            db = self._get_connect()
            cursor = db.cursor()
            index: int = len(params) // step + 1

            for i in range(index):
                sub_params: list = params[i * step:(i + 1) * step]
                rows = cursor.executemany(sql, sub_params)
                db.commit()
                logger.info(f"Insert db successfully. Rows: {rows}.")
        except Exception as e:
            db.rollback()
            logger.error(f"Insert db error. Error: {e}.")
        finally:
            cursor.close()
            db.close()

    def _exec(self, sql: str) -> None:
        try:
            db = self._get_connect()
            cursor = db.cursor()
            cursor.execute(sql)
            db.commit()
            logger.info(f"Exec db successfully.")
        except Exception as e:
            db.rollback()
            logger.error(f"Exec db error. Error: {e}.")
        finally:
            cursor.close()
            db.close()

    # def qc_select_auto(self, stcd='CGB086070', ymdhm_start='2023-03-26 10:00', ymdhm_end='2023-04-01 08:00'):
    #     """
    #     从SQL Server数据库的自动预报结果表中查询指定时段的某断面流量
    #     目前只考虑了调用预报结果的数据库，未来需要考虑调用水库调度结果的数据库
    #     :param stcd: 断面编码
    #     :param ymdhm_start: 开始时间：按照yyyy-mm-dd hh:00格式，为整点时刻数据
    #     :param ymdhm_end: 结束时间：按照yyyy-mm-dd hh:00格式，为整点时刻数据
    #     :return: 返回指定时段的某断面流量列表，供RasHandler.sql2file方法读取，注意要求不能超过6位（若有小数点，小数点也算1位）
    #     """
    #     sql = f"SELECT QC FROM [Feixian].[dbo].[YB_Totflow] where stcd='{stcd}' and ymdhm>='{ymdhm_start}' and " \
    #           f"ymdhm<='{ymdhm_end}' order by ymdhm"
    #     self.cursor.execute(sql)
    #     row = self.cursor.fetchone()
    #     qc_list = list()
    #     while row:
    #         qc_list.append(row[0])
    #         row = self.cursor.fetchone()
    #     self.conn.close()
    #     return qc_list

    def qc_select_manual(self, stcd='CGB086070', honghaoname='2023032810洪水', username='admin'):
        """
        从SQL Server数据库的人工干预结果表中查询指定时段的某断面流量
        注意此方法需要通过查询得到起止时间，如何传出去？
        :param stcd: 断面编码
        :param honghaoname: 洪水名称
        :param username: 操作用户名
        :return: 返回一个包含起止时间列表和预报流量列表的字典manual_dict
        """
        conn = self._get_connect()
        cursor = conn.cursor()
        sql = f"SELECT ymdhm,QC FROM `water_disasters_feixian1`.`siyu_yubao_data` where stcd='{stcd}' and Honghaoname='{honghaoname}' and " \
              f"Username='{username}' order by ymdhm"
        cursor.execute(sql)
        row = cursor.fetchone()
        time_list = list()
        qc_list = list()
        while row:
            time_list.append(row[0].strftime('%Y-%m-%d %H:%M'))
            qc_list.append(str(row[1]))
            row = cursor.fetchone()
        start_end_time_list = list()
        start_end_time_list.append(time_list[0])
        start_end_time_list.append(time_list[-1])
        manual_dict = {'time': start_end_time_list, 'qc': qc_list}
        conn.close()
        return manual_dict

    def xq_diaodu_select(self, table, stcd):
        """
        查询调度结果
        :param stcd: 断面编码，string
        :return: 返回流量结果列表
        """
        conn = self._get_connect()
        cursor = conn.cursor()
        sql = f"SELECT Q FROM `{self.database}`.`{table}` where stcd='{stcd}'"
        cursor.execute(sql)
        row = cursor.fetchone()
        xq_list = list()
        while row:
            xq_list.append(str(row[0]))
            row = cursor.fetchone()
        conn.close()
        return xq_list

    def q_from_table(self, reservoir, scheme_name, ymdhm_start, ymdhm_end):
        """
        从两个表中联合查询指定水库的出库流量过程
        :param reservoir: 水库名称
        :param scheme_name: 方案名称
        :param ymdhm_start: 开始时间
        :param ymdhm_end: 结束时间
        :return: 包含流量过程的一维list
        """
        conn = self._get_connect()
        cursor = conn.cursor()
        query = """
        SELECT 
            ssd.DATAVALUE
        FROM 
            SD_DSP_SCHEME_INFO ssi
        JOIN 
            SD_DSP_SCHEME_DATA ssd
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
        # 处理结果为一维列表
        flow_values = [row[0] for row in results]
        conn.close()
        return flow_values

    def depth_to_mysql(self, table, time_list, depth_data: np.ndarray):
        sql = f"Insert Into {table} (`id`,`ymdhm`,`depth`) values (%s, %s, %s)"
        params: list = []
        for time_step, row in enumerate(depth_data):
            for i, data in enumerate(row):
                # if data < 0.01:
                #     continue
                params.append([i, time_list[time_step], round(data, 3)])

        self._batch_insert(sql, params)

    def q_to_mysql(self, table, time_list, q_data: np.ndarray):
        sql = f"Insert Into {table} (`ymdhm`,`Q`) values (%s, %s)"
        params: list = []
        for time_step, data in enumerate(q_data):
            # if data < 0.01:
            #     continue
            params.append([time_list[time_step], round(data, 3)])

        self._batch_insert(sql, params)


# 下面是用于临时测试的代码
if __name__ == '__main__':
    server = '192.168.0.18'
    username = 'root'
    password = 'zhy@911!'
    db = 'water_disasters_feixian1'
    port = 5306
    mysql_handler = MySQLHandler(server, username, password, port, db)
    # logger.info(mysql_handler.qc_select_manual())
    # logger.info(sqlserver_handler.qc_select_manual())
    depth_data = np.array([[1.98, 2, 3, 4.4, 5], [2, 3, 4, 5.98, 6], [
                          3, 4, 5.1, 0.001, 0.0002], [2.3, 4, 8, 19, 20], [0.0001, 2, 7, 8, 9]])
    logger.info(depth_data)
