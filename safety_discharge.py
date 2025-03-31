# -*- coding: utf-8 -*-
# @Time    : 2025/3/29 下午3:31
# @Author  : wm
# @Software   : PyCharm
"""
计算72小时恒定流条件（100流量计算至10100，每200一步）下的指定网格是否淹没，返回淹没时的流量
两河口：14569
霍山县中学：8340
青山乡：1944
下符桥镇政府：5785
迎驾酒厂：10732
"""
import os
import csv
import numpy as np
from to_csv import insert_time_and_save_to_csv
from hdf_handler import HDFHandler
from post_processor import PostProcessor
from ras_handler import RASHandler
from time_format_converter import TimeFormatConverter
from config_ubuntu import *
import logging
from log_handler import ImmediateFileHandler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = ImmediateFileHandler('safety_discharge.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

b01_path = os.path.join(RAS_PATH, f"FZLall.b01")
p01_hdf_path = os.path.join(RAS_PATH, f"FZLall.p01.hdf")
output_path = "/home/v01dwm/safety_discharge_results"

ymdhm_start = "2025-03-20 08:00"
ymdhm_end = "2025-03-23 07:00"

FID = [26502, 8340, 2016, 5785, 10732]
FID_name = ["两河口", "霍山县中学", "青山乡", "下符桥镇政府", "迎驾酒厂"]
# 存储各点的安全泄量
flood_Q = {name: None for name in FID_name}

for i in range(100, 10100, 300):
    logger.info(f"开始模拟Q = {i}的工况...")
    # 构造72小时恒定流ndarray
    xq_list = np.ones(72) * i
    try:
        logger.info(f"将Q = {i}写入佛子岭水库边界条件中...")
        # 修改边界条件
        ras_handler = RASHandler(xq_list)
        time_format_converter = TimeFormatConverter()
        # 修改b01文件
        start_time_b01_and_hdf = time_format_converter.convert(ymdhm_start, 'b01')
        end_time_b01_and_hdf = time_format_converter.convert(ymdhm_end, 'b01')
        ras_handler.modify_b01(b01_path, b01_path, start_time_b01_and_hdf, end_time_b01_and_hdf, '1MIN', '1HOUR')

        # 修改.p01.hdf文件，修改其中的边界条件并把Results删除后改名为.p01.tmp.hdf
        hdf_handler = HDFHandler(p01_hdf_path, ymdhm_start, ymdhm_end)
        # 修改佛子岭水库出库边界
        hdf_handler.modify_boundary_conditions_safety_discharge(xq_list, start_time_b01_and_hdf, end_time_b01_and_hdf)

        # 获取符合hdf_handler.modify_plan_data方法要求的start_date和end_date，为该方法的调用做好准备
        start_time_plan_data = time_format_converter.convert(ymdhm_start, 'simulation')
        end_time_plan_data = time_format_converter.convert(ymdhm_end, 'simulation')
        # 修改p01.hdf文件中的Plan Data->Plan Information中的Simulation End Time、Simulation Start Time和Time Window
        hdf_handler.modify_plan_data(start_time_plan_data, end_time_plan_data)

        # 得到.p01.tmp.hdf供Linux ras调用
        hdf_handler.remove_hdf_results()
    except Exception as e:
        logger.error(e)

    # 调用run_unsteady.sh进行计算
    try:
        logger.info("开始调用HEC-RAS计算...")
        sh_path = os.path.dirname(b01_path) + os.path.sep + 'run_unsteady.sh'
        return_code = ras_handler.run_model(sh_path)
        logger.info("HEC-RAS计算完成")
    except Exception as e:
        logger.error(e)

    try:
        logger.info("开始提取水深数据......")
        # 从.p01.hdf结果文件中读取需要的数据
        cells_minimum_elevation_data = hdf_handler.read_dataset(
            'Cells Minimum Elevation')
        wse_data = hdf_handler.read_dataset('Water Surface')

        post_processor = PostProcessor()
        # 将Cells中多余的高程为nan的空网格删去
        real_mesh = post_processor.get_real_mesh(cells_minimum_elevation_data)
        # 调用generating_depth方法，用水位减去高程，即得水深值
        depth_data = post_processor.generating_depth(
            cells_minimum_elevation_data, wse_data, real_mesh)
        logger.info("水深数据提取已完成")
        # 查找给定网格是否淹没
        for index, cell in enumerate(FID):
            # 认为水深>=0.1m时算作淹没
            if not (depth_data[:, cell] < 0.1).all():
                if flood_Q[FID_name[index]] is None:
                    logger.info(f"保证 {FID_name[index]} 不淹没的安全泄量为Q = {i}")
                    flood_Q[FID_name[index]] = i

        # csv_path = output_path + os.path.sep + f"output_{i}.csv"
        # # 将水深计算结果输出为csv文件
        # insert_time_and_save_to_csv(depth_data, csv_path)
        # logger.info(f"水深数据已写入到{csv_path}")

        # 判断是否已经得到所有的安全泄量
        if all(value is not None for value in flood_Q.values()):
            break
        for key, value in flood_Q.items():
            if value is not None:
                logger.info(f"{key}的安全泄量为Q = {value}")
        logger.info("------------------------")
    except Exception as e:
        logger.error(e)

# 将安全泄量写入到csv文件中
csv_file = output_path + os.path.sep + "safety_discharge.csv"
headers = ["Name", "Q_safe"]
# 写入文件
with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:  # 注意编码和换行符
    writer = csv.writer(f)
    writer.writerow(headers)  # 写入表头
    for key, value in flood_Q.items():
        writer.writerow([key, value])  # 逐行写入键值对
