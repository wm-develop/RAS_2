# -*- coding: utf-8 -*-
# @Time    : 2024/10/23 上午9:40
# @Author  : wm
# @Software   : PyCharm
"""
将从hdf中读取的水深数据写入csv表中
"""
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from config import *
from hdf_handler import HDFHandler
from post_processor import PostProcessor


def insert_time_and_save_to_csv(data_array, output_file):
    """
    在给定的二维数组前插入时间列，并保存到 Excel 文件。

    参数:
    data_array (ndarray): 二维数据数组。
    output_file (str): 输出 Excel 文件的文件名。

    返回:
    None
    """

    # 将结果转换为 DataFrame
    df = pd.DataFrame(data_array)
    df.to_csv(output_file, header=False, index=False)


# 测试
if __name__ == '__main__':
    p01_hdf_path = os.path.join(RAS_PATH_WIN, f"FZLall.p01.hdf")
    hdf_handler = HDFHandler(p01_hdf_path)
    cells_minimum_elevation_data = hdf_handler.read_dataset(
        'Cells Minimum Elevation')
    wse_data = hdf_handler.read_dataset('Water Surface')
    post_processor = PostProcessor()
    # 将Cells中多余的高程为nan的空网格删去
    real_mesh = post_processor.get_real_mesh(cells_minimum_elevation_data)
    # 调用generating_depth方法，用水位减去高程，即得水深值
    depth_data = post_processor.generating_depth(
        cells_minimum_elevation_data, wse_data, real_mesh)
    file_name = 'output.csv'  # 输出文件名

    insert_time_and_save_to_csv(depth_data, file_name)
