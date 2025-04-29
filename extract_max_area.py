# -*- coding: utf-8 -*-
# @Time    : 2025/4/29 下午5:10
# @Author  : wm
# @Software   : PyCharm
"""从hdf文件提取最大淹没面积"""
import os
import h5py
import geopandas as gpd
import numpy as np
from logger import logger
from hdf_handler import HDFHandler
from post_processor import PostProcessor
from to_csv import insert_time_and_save_to_csv
from time_format_converter import TimeFormatConverter


RAS_PATH = r"D:\Desktop\20250415fzl\Fzlmodel20250429"
p01_hdf_path = os.path.join(RAS_PATH, f"FZLall.p01.hdf")
ymdhm_start = "2025-04-09 00:00"
ymdhm_end ="2025-04-11 23:00"
output_path = r"D:\Desktop\1"

try:
    hdf_handler = HDFHandler(p01_hdf_path, ymdhm_start, ymdhm_end)
    time_format_converter = TimeFormatConverter()
    logger.info("开始将水深数据存储到csv文件中......")
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

    csv_path = output_path + os.path.sep + "output.csv"
    # 将水深计算结果输出为csv文件
    insert_time_and_save_to_csv(depth_data, csv_path)
    logger.info(f"水深数据已写入到{csv_path}")
except Exception as e:
    logger.error(e)

try:
    logger.info("正在提取坝下水位过程...")
    bailianya_dam_depth_path = output_path + os.path.sep + "bailianya.csv"
    mozitan_dam_depth_path = output_path + os.path.sep + "mozitan.csv"
    foziling_dam_depth_path = output_path + os.path.sep + "foziling.csv"
    bailianya_grids = [25495, 25494, 25496, 25492]
    mozitan_grids = [24834, 24833, 24835, 24832]
    foziling_grids = [24418, 24417, 17369, 17367]
    water_level_array = post_processor.get_water_level(wse_data, real_mesh)
    bailianya_dam_depth_result = post_processor.calculate_and_save_row_means(water_level_array,
                                                                             bailianya_dam_depth_path, bailianya_grids)
    mozitan_dam_depth_result = post_processor.calculate_and_save_row_means(water_level_array, mozitan_dam_depth_path,
                                                                           mozitan_grids)
    foziling_dam_depth_result = post_processor.calculate_and_save_row_means(water_level_array, foziling_dam_depth_path,
                                                                            foziling_grids)
    if bailianya_dam_depth_result == 1 or mozitan_dam_depth_result == 1 or foziling_dam_depth_result == 1:
        raise RuntimeError("Failed: 提取坝下水位过程中出现错误")
    logger.info("坝下水位过程提取成功")
except Exception as e:
    logger.error(e)

# 下面是算最大淹没面积的发生时刻
try:
    # 修改为研究区的shp文件
    logger.info("正在计算最大淹没面积...")
    shp1 = RAS_PATH + os.path.sep + 'demo2' + os.path.sep + 'demo2.shp'
    gdf = gpd.read_file(shp1)
    # 提取属性表并保存为矩阵形式
    attributes_df = gdf
    # 重新排列列顺序
    columns = attributes_df.columns.tolist()
    new_order = [columns[3]]
    reordered_df = attributes_df[new_order]
    area_data = reordered_df.to_numpy()

    depth_data_transposed = depth_data.T
    # depth_data_final是一个二维ndarray，行代表网格FID，列代表时间步。第一列内容为每个网格的面积，后面的内容为每个网格在每个时间步的水深值
    depth_data_final = np.hstack((area_data, depth_data_transposed))

    # num_shapefiles为计算时间的间隔数
    num_shapefiles = len(depth_data)
    num_ymdhm = int(time_format_converter.calculate_intervals(ymdhm_start, ymdhm_end))
    logger.info(f"num_intervals: {num_shapefiles}, num_ymdhm: {num_ymdhm}, type: {type(num_shapefiles)}")
    depth_count = np.zeros((len(depth_data_final), num_shapefiles))  # 新建空数组用来计算每个单元的受淹面积
    # depth_count的行代表网格FID，列代表时间步
    for i in range(len(depth_data_final)):  # 遍历每个网格
        for j in range(num_shapefiles):  # 遍历每个时间步
            x = depth_data_final[i, j + 1]  # 获取水深值（跳过面积列）
            if x > 0.2:  # 仅当水深>0.2m时才认为是淹没
                depth_count[i][j] = depth_data_final[i][0] * 0.001 * 0.001  # 面积
            else:
                depth_count[i][j] = 0
    depth_count_final = depth_count.sum(axis=0)
    # 找到最大值及其索引
    max_index = np.argmax(depth_count_final)
    attributes_df[f'depth_{max_index}'] = depth_data_final[:, max_index + 1]

    # 输出为shp格式并保存
    shp_path = output_path + os.path.sep + "max_water_area.shp"
    gdf.to_file(shp_path)
    logger.info(f"最大淹没面积shp文件已保存到{shp_path}")
except Exception as e:
    logger.error(e)
    if "Failed to create Shape DBF file" in str(e):
        logger.error("无法创建shp文件，请确保shp文件及其相关文件未被其他程序打开")
