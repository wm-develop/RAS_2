# -*- coding: utf-8 -*-
# @Time    : 2025/8/8 09:15
# @Author  : wm
# @Software   : PyCharm
"""
将HEC-RAS中处理过的DEM数据插值到shp网格中
"""
import h5py
import geopandas as gpd
import numpy as np
from post_processor import PostProcessor


def read_dataset(filepath, dataset_name: str):
    """
    从HDF文件中读取dataset中的数据
    :param dataset_name: dataset的名称
    :return: 返回读取到的数据集，类型为np.ndarray
    """
    f = h5py.File(filepath, 'r')
    # numpy.ndarray
    if dataset_name == 'Water Surface':
        wse_data = \
            f['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['2D Flow Areas'][
                'Perimeter 1']['Water Surface'][:]
        return wse_data
    # 查找路径错误，需要重新换目录
    # elif dataset_name == 'Velocity X':
    #     node_velocity_x_data = \
    #     f['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['2D Flow Areas'][
    #         'Perimeter 1']['Node Velocity - Velocity X'][:]
    #     return node_velocity_x_data
    # elif dataset_name == 'Velocity Y':
    #     node_velocity_y_data = \
    #     f['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['2D Flow Areas'][
    #         'Perimeter 1']['Node Velocity - Velocity Y'][:]
    #    return node_velocity_y_data
    elif dataset_name == "Cells Minimum Elevation":
        cells_minimum_elevation_data = f['Geometry']['2D Flow Areas']['Perimeter 1']['Cells Minimum Elevation'][:]
        return cells_minimum_elevation_data
    elif dataset_name == "FacePoints Coordinate":
        facepoints_coordinate_data = f['Geometry']['2D Flow Areas']['Perimeter 1']['FacePoints Coordinate'][:]
        return facepoints_coordinate_data
    elif dataset_name == "Cells Center Coordinate":
        cells_coordinate_data = f['Geometry']['2D Flow Areas']['Perimeter 1']['Cells Center Coordinate'][:]
        return cells_coordinate_data
    elif dataset_name == "Cells FacePoint Indexes":
        cells_facepoint_indexes_data = f['Geometry']['2D Flow Areas']['Perimeter 1']['Cells FacePoint Indexes'][:]
        return cells_facepoint_indexes_data
    elif dataset_name == "Outflow":
        heng_outflow_data = \
            f['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series'][
                '2D Flow Areas']['Perimeter 1']['Boundary Conditions']['Hengpaitou Outflow'][:, 1]
    return heng_outflow_data


def add_elevation_to_shp(hdf_filepath, shp_filepath, output_shp_filepath=None):
    """
    将HDF文件中的高程值添加到SHP文件中
    :param hdf_filepath: HDF文件路径
    :param shp_filepath: 输入SHP文件路径
    :param output_shp_filepath: 输出SHP文件路径，如果为None则覆盖原文件
    """
    try:
        # 从HDF文件中读取高程数据
        print("正在读取HDF文件中的高程数据...")
        cells_minimum_elevation_data = read_dataset(hdf_filepath, 'Cells Minimum Elevation')
        post_processor = PostProcessor()
        real_mesh = post_processor.get_real_mesh(cells_minimum_elevation_data)
        cells_minimum_elevation_data = cells_minimum_elevation_data[:real_mesh]
        print(f"读取到 {len(cells_minimum_elevation_data)} 个高程值")

        # 读取SHP文件
        print("正在读取SHP文件...")
        gdf = gpd.read_file(shp_filepath)
        print(f"SHP文件包含 {len(gdf)} 个要素")

        # 检查数据长度是否匹配
        if len(cells_minimum_elevation_data) != len(gdf):
            print(f"警告：高程数据长度({len(cells_minimum_elevation_data)})与SHP要素数量({len(gdf)})不匹配")
            # 取较小的长度以避免索引错误
            min_length = min(len(cells_minimum_elevation_data), len(gdf))
            cells_minimum_elevation_data = cells_minimum_elevation_data[:min_length]
            gdf = gdf.iloc[:min_length].copy()
            print(f"已截取前 {min_length} 个数据进行处理")

        # 添加高程字段
        print("正在添加高程字段...")
        gdf['elevation'] = cells_minimum_elevation_data

        # 确定输出路径
        if output_shp_filepath is None:
            output_shp_filepath = shp_filepath

        # 保存结果
        print(f"正在保存结果到 {output_shp_filepath}...")
        gdf.to_file(output_shp_filepath)

        print("处理完成！")
        print(f"高程字段统计信息：")
        print(f"  最小值: {gdf['elevation'].min():.2f}")
        print(f"  最大值: {gdf['elevation'].max():.2f}")
        print(f"  平均值: {gdf['elevation'].mean():.2f}")

        return gdf

    except Exception as e:
        print(f"处理过程中出现错误: {str(e)}")
        raise


def setDEMtoSHP():
    """
    主函数：将DEM数据设置到SHP文件中
    """
    hdf_file_path = r'D:\Desktop\HQHmodel_0811F7\HQHmodel.p01.hdf'
    shp_file_path = r'E:\Workspace\PythonPractice\hqh\cahmhec\RAS_2\HQH底高程SHP\c1_SpatialJoin.shp'

    # 可以指定输出文件路径，如果不指定则覆盖原文件
    # output_shp_path = r'D:\Desktop\HQHmodel_0807F2\1_with_elevation.shp'
    output_shp_path = None  # 覆盖原文件

    # 执行处理
    result_gdf = add_elevation_to_shp(hdf_file_path, shp_file_path, output_shp_path)

    return result_gdf


if __name__ == "__main__":
    # 运行主函数
    setDEMtoSHP()