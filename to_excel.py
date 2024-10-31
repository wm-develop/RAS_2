# -*- coding: utf-8 -*-
# @Time    : 2024/10/22 上午11:54
# @Author  : wm
# @Software   : PyCharm
"""
将从hdf中读取的水深数据写入excel表中
"""
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from config import *
from hdf_handler import HDFHandler
from post_processor import PostProcessor


def insert_time_and_save_to_excel(data_array, start_time_str, output_file):
    """
    在给定的二维数组前插入时间列，并保存到 Excel 文件。

    参数:
    data_array (ndarray): 二维数据数组。
    start_time_str (str): 起始时间字符串，格式为 'YYYY-MM-DD HH:MM'。
    output_file (str): 输出 Excel 文件的文件名。

    返回:
    None
    """
    # 转换起始时间字符串为 datetime 对象
    start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M')

    # 生成时间序列
    num_rows = data_array.shape[0]
    time_series = [start_time + timedelta(minutes=10 * i) for i in range(num_rows)]

    # 将时间序列转换为字符串格式
    time_series_str = [dt.strftime('%Y-%m-%d %H:%M') for dt in time_series]

    # 将时间序列插入到数组的最前面
    result_array = np.column_stack((time_series_str, data_array))

    # 将结果转换为 DataFrame
    df = pd.DataFrame(result_array, columns=['时间'] + [f'{i}' for i in range(0, data_array.shape[1])])

    # 每个工作表最多列数
    max_columns_per_sheet = 10000 - 1  # 减去时间列

    # 分批写入
    with pd.ExcelWriter(output_file) as writer:
        for i in range(0, df.shape[1] - 1, max_columns_per_sheet):
            end = i + max_columns_per_sheet
            sheet_name = f'Sheet{i // max_columns_per_sheet + 1}'
            # 每张表还需包含时间列
            df_to_write = df.iloc[:, [0] + list(range(i + 1, min(end + 1, df.shape[1])))]
            df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"数据已保存到 {output_file}")


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
    start_time = '2024-08-30 22:00'  # 示例起始时间
    file_name = 'output.xlsx'  # 输出文件名

    insert_time_and_save_to_excel(depth_data, start_time, file_name)
