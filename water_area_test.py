# -*- coding: utf-8 -*-
# @Time : 2024/10/22 上午11:22
# @Author : Gamino


import geopandas as gpd
import numpy as np
from hdf_handler import HDFHandler
from post_processor import PostProcessor
import time


# 单位为平方千米
def compute_losses0(a, b, num_shapefiles):
    for i in range(len(a)):
        for j in range(num_shapefiles):
            x = a[i, j + 1]
            if x > 0:
                b[i][j] = a[i][0] * 0.001 * 0.001
            else:
                b[i][j] = 0
    return b


def main():
    start = time.perf_counter()
    shp1 = '/home/v01dwm/Foziling_Model_1030/demo/demo_20241022.shp'
    gdf = gpd.read_file(shp1)
    # 提取属性表并保存为矩阵形式
    attributes_df = gdf

    # 重新排列列顺序
    columns = attributes_df.columns.tolist()
    new_order = [columns[3]]
    reordered_df = attributes_df[new_order]
    area_data = reordered_df.to_numpy()

    p01_hdf_path = '/home/v01dwm/Foziling_Model_1030/FZLall.p01.hdf'
    hdf_handler = HDFHandler(p01_hdf_path, "2024-10-27 01:00", "2024-10-30 00:00")
    cells_minimum_elevation_data = hdf_handler.read_dataset(
        'Cells Minimum Elevation')
    wse_data = hdf_handler.read_dataset('Water Surface')
    post_processor = PostProcessor()
    # 将Cells中多余的高程为nan的空网格删去
    real_mesh = post_processor.get_real_mesh(cells_minimum_elevation_data)
    # 调用generating_depth方法，用水位减去高程，即得水深值
    depth_data = post_processor.generating_depth(
        cells_minimum_elevation_data, wse_data, real_mesh)

    depth_data_transposed = depth_data.T

    depth_data_final = np.hstack((area_data, depth_data_transposed))

    num_shapefiles = 427
    depth_count = np.zeros((len(depth_data_final), num_shapefiles))  # 新建空数组用来计算每个单元的受淹面积

    depth_count = compute_losses0(depth_data_final, depth_count, num_shapefiles)
    depth_count_final = depth_count.sum(axis=0)

    # 找到最大值及其索引
    max_value = np.max(depth_count_final)
    max_index = np.argmax(depth_count_final)
    print(max_value)
    print(max_index)

    attributes_df[f'depth_{max_index}'] = depth_count[:, max_index]
    output_path = 'demo.shp'
    gdf.to_file(output_path)

    end = time.perf_counter()
    runtime = end - start
    print("运行时间为", runtime, "秒")




if __name__ == "__main__":
    main()
