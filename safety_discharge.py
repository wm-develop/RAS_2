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
from ras_handler_safety_discharge import RASHandler
from time_format_converter import TimeFormatConverter
from config_ubuntu import *
import logging
from log_handler import ImmediateFileHandler


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = ImmediateFileHandler('safety_discharge.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

b01_path = os.path.join(RAS_PATH, f"FZLall.b01")
p01_hdf_path = os.path.join(RAS_PATH, f"FZLall.p01.hdf")
output_path = "/home/v01dwm/safety_discharge_results"

ymdhm_start = "2025-03-22 08:00"
ymdhm_end = "2025-03-25 07:00"

FID_lianghekou = [2183, 2184, 2185, 2186, 2187, 2188, 2189,
                  2252, 2253, 2254, 2255, 2256, 2257, 2258,
                  2325, 2326, 2327, 2328, 2329, 2330, 2331,
                  2404, 2405, 2406, 2407, 2408, 2409, 2410,
                  2485, 2486, 2487, 2488, 2489, 2490, 2491,
                  2563, 2564, 2565, 2566, 2567, 2568, 2569,
                  2637, 2638, 2639, 2640, 2641, 2642, 2643]
FID_huoshanzhongxue = [7916, 7917, 7918, 7919, 7920, 7921, 7922, 7923,
                       7989, 7990, 7991, 7992, 7993, 7994, 7995, 7996,
                       8058, 8059, 8060, 8061, 8062, 8063, 8064, 8065,
                       8118, 8119, 8120, 8121, 8122, 8123, 8124, 8125,
                       8174, 8175, 8176, 8177, 8178, 8179, 8180, 8181,
                       8227, 8228, 8229, 8230, 8231, 8232, 8233, 8234,
                       8278, 8279, 8280, 8281, 8282, 8283, 8284, 8285,
                       8328, 8329, 8330, 8331, 8332, 8333, 8334, 8335,
                       8385, 8384, 8383, 8382, 8381, 8380,
                       8431, 8430, 8429, 8428, 8427,
                       8476, 8475, 8518]
FID_qingshanxiang = [2014, 2015, 2064, 2016, 1974, 1975, 1976, 1977, 1942, 1943, 1944, 1918, 1919, 1904, 1903, 1892, 1868, 1881]
FID_xiafuqiao = [5748, 5749, 5784, 5785, 5713, 5714, 5820]
FID_yingjia = [10678, 10660, 10646, 10634, 10635, 10624, 10625, 10626, 10627, 10640, 10653, 10669, 10689, 10708, 10723,
               10697, 10715, 10698, 10679, 10716, 10699, 10680, 10661, 10700, 10681, 10662, 10647, 10701, 10682, 10663, 10648,
               10717, 10702, 10683, 10664, 10649, 10636, 10718, 10703, 10684, 10665, 10650, 10637, 10732, 10719, 10704, 10685, 10666, 10651, 10638,
               10733, 10720, 10705, 10686, 10667, 10652, 10639, 10746, 10734, 10721, 10706, 10687, 10668,
               10747, 10735, 10722, 10707, 10688, 10748, 10736]
# FID = [2326, 8384, 2016, 5748, 10666]
# FID_name = ["两河口", "霍山县中学", "青山乡", "下符桥镇政府", "迎驾酒厂"]
# # 存储各点的安全泄量
# flood_Q = {name: None for name in FID_name}

for i in range(100, 9000, 300):
    logger.info(f"开始模拟Q = {i}的工况...")
    print(f"开始模拟Q = {i}的工况...")
    # 构造72小时+12小时恒定流ndarray
    xq_list = np.ones(84) * i
    xq_list_half = xq_list * 0.5
    xq_list_xhd = np.zeros(84)

    # 佛子岭水库的泄流过程推迟12小时，以确保水库中有足够的水量
    xq_list[:12] = 0

    try:
        logger.info(f"将Q = {i}写入佛子岭水库边界条件中...")
        print(f"将Q = {i}写入佛子岭水库边界条件中...")
        # 修改边界条件
        ras_handler = RASHandler(xq_list)
        time_format_converter = TimeFormatConverter()
        # 修改b01文件
        start_time_b01_and_hdf = time_format_converter.convert(ymdhm_start, 'b01')
        end_time_b01_and_hdf = time_format_converter.convert(ymdhm_end, 'b01')
        ras_handler.modify_b01(b01_path, b01_path, start_time_b01_and_hdf, end_time_b01_and_hdf, '30SEC', '1HOUR')

        # 修改.p01.hdf文件，修改其中的边界条件并把Results删除后改名为.p01.tmp.hdf
        hdf_handler = HDFHandler(p01_hdf_path, ymdhm_start, ymdhm_end)
        # 修改佛子岭水库出库边界
        hdf_handler.modify_boundary_conditions_with_xhd_hpt_rating_curve(xq_list_half, xq_list_half, xq_list, xq_list_xhd, start_time_b01_and_hdf, end_time_b01_and_hdf)

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
        print("开始调用HEC-RAS计算...")
        sh_path = os.path.dirname(b01_path) + os.path.sep + 'run_unsteady.sh'
        return_code = ras_handler.run_model(sh_path)
        logger.info("HEC-RAS计算完成")
        print("HEC-RAS计算完成")
    except Exception as e:
        logger.error(e)

    try:
        logger.info("开始提取水深数据......")
        print("开始提取水深数据......")
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
        print("水深数据提取已完成")
        logger.info(f"Q = {i}条件下，depth_data中总共含有{depth_data.shape[0]}行数据")
        print(f"Q = {i}条件下，depth_data中总共含有{depth_data.shape[0]}行数据")

        # 提取5个点附近的网格的水深时序数据
        locations = {
            "lianghekou": FID_lianghekou,
            "huoshanzhongxue": FID_huoshanzhongxue,
            "qingshanxiang": FID_qingshanxiang,
            "xiafuqiao": FID_xiafuqiao,
            "yingjia": FID_yingjia
        }
        # 遍历字典，逐个生成CSV文件
        for name, fid in locations.items():
            # 提取对应列的数据
            data = depth_data[:, fid]

            # ---------- 核心计算逻辑 ----------
            # 计算每行的和
            row_sums = np.sum(data, axis=1)
            # 找到最大行和的索引
            max_row_index = np.argmax(row_sums)
            # 提取该行的数据
            max_row_data = data[max_row_index, :]
            # 计算行和的最大值
            max_row_sum = row_sums[max_row_index]
            # 计算该行的平均值
            max_row_mean = np.mean(max_row_data)
            # ---------------------------------
            logger.info(f"在Q = {i}的条件下，{name}区域内的最大水深和为{max_row_sum}，最大平均水深为{max_row_mean}")
            print(f"在Q = {i}的条件下，{name}区域内的最大水深和为{max_row_sum}，最大平均水深为{max_row_mean}")

            # 生成文件名（如 "lianghekou.csv"）
            filename = os.path.join(output_path, f"{name}_{i}.csv")

            # 写入CSV
            with open(filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(fid)  # 列标题为列索引列表
                writer.writerows(data)  # 直接写入NumPy数组
            # logger.info(f"{name}区域对应Q = {i}时的水深时序数据已保存到{name}_{i}.csv文件中")

        # # 查找给定网格是否淹没
        # for index, cell in enumerate(FID):
        #     # 认为水深>=0.1m时算作淹没
        #     if not ((depth_data[:, cell] < 0.1).all()):
        #         if flood_Q[FID_name[index]] is None:
        #             logger.info(f"保证 {FID_name[index]} 不淹没的安全泄量为Q = {i}")
        #             flood_Q[FID_name[index]] = i

        # csv_path = output_path + os.path.sep + f"output_{i}.csv"
        # # 将水深计算结果输出为csv文件
        # insert_time_and_save_to_csv(depth_data, csv_path)
        # logger.info(f"水深数据已写入到{csv_path}")

        # # 判断是否已经得到所有的安全泄量
        # if all(value is not None for value in flood_Q.values()):
        #     break
        # for key, value in flood_Q.items():
        #     if value is not None:
        #         logger.info(f"{key}的安全泄量为Q = {value}")
        logger.info("------------------------")
        print("------------------------")
    except Exception as e:
        logger.error(e)

# # 将安全泄量写入到csv文件中
# csv_file = "safety_discharge.csv"
# headers = ["Name", "Q_safe"]
# # 写入文件
# with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:  # 注意编码和换行符
#     writer = csv.writer(f)
#     writer.writerow(headers)  # 写入表头
#     for key, value in flood_Q.items():
#         writer.writerow([key, value])  # 逐行写入键值对
