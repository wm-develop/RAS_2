# -*- coding: utf-8 -*-
# @Time    : 2024/10/14 下午5:15
# @Author  : wm
# @Software   : PyCharm
"""
从p01.hdf文件中读取水深数据，并将结果写入shp网格的属性表中
"""
from hdf_handler import HDFHandler
from post_processor import PostProcessor
import numpy as np
import geopandas as gpd
import os


# 从.p01.hdf结果文件中读取需要的数据
hdf_handler = HDFHandler(r'D:\Desktop\Foziling_Model_1013\FZLall.p01.hdf')
cells_minimum_elevation_data = hdf_handler.read_dataset(
    'Cells Minimum Elevation')
wse_data = hdf_handler.read_dataset('Water Surface')

post_processor = PostProcessor()
# 将Cells中多余的高程为nan的空网格删去
real_mesh = post_processor.get_real_mesh(cells_minimum_elevation_data)
# 调用generating_depth方法，用水位减去高程，即得水深值
depth_data = post_processor.generating_depth(cells_minimum_elevation_data, wse_data, real_mesh)

# 读取原始 shapefile
shapefile_path = r'D:\Desktop\Foziling_Model_1013\321.shp'
gdf = gpd.read_file(shapefile_path)

# 检查 shapefile 的记录数是否与 depth_data 的列数相匹配
if len(gdf) != depth_data.shape[1]:
    raise ValueError("Shapefile feature count does not match the depth data column count.")

# 输出目录
output_dir = 'output_shapefiles'
os.makedirs(output_dir, exist_ok=True)

# 遍历每个时间步
for i in range(depth_data.shape[0]):
    # 为当前时间步创建一个新的 GeoDataFrame
    gdf_copy = gdf.copy()

    # 添加新的列，命名为 'Depth_Timestep_i'，并将当前时间步的水深数据填入
    gdf_copy[f'Depth_Timestep_{i}'] = depth_data[i]

    # 构建输出文件路径
    output_shapefile_path = os.path.join(output_dir, f'depth_timestep_{i}.shp')

    # 保存新的 shapefile
    gdf_copy.to_file(output_shapefile_path, driver='ESRI Shapefile')

print("Shapefiles generated successfully.")
