# -*- coding: UTF-8 -*-
import h5py
import numpy as np
from datetime import datetime


def convert_time_date_stamp(time_date_stamp_array):
    """
    将HEC-RAS的时间格式转换为标准格式
    从 '09APR2025 00:10:00' 转换为 'YYYY-MM-DD HH:MM:SS'
    
    :param time_date_stamp_array: 原始时间戳数组
    :return: 转换后的时间戳数组
    """
    converted_times = []
    
    for time_bytes in time_date_stamp_array:
        # 解码字节串为字符串
        if isinstance(time_bytes, bytes):
            time_str = time_bytes.decode('utf-8').strip()
        else:
            time_str = str(time_bytes).strip()
        
        # 解析格式: '09APR2025 00:10:00'
        try:
            dt = datetime.strptime(time_str, '%d%b%Y %H:%M:%S')
            # 转换为 'YYYY-MM-DD HH:MM:SS' 格式
            converted_str = dt.strftime('%Y-%m-%d %H:%M:%S')
            converted_times.append(converted_str.encode('utf-8'))
        except Exception as e:
            # 如果转换失败，保留原始值
            print(f"Warning: Failed to convert time '{time_str}': {e}")
            converted_times.append(time_bytes if isinstance(time_bytes, bytes) else time_bytes.encode('utf-8'))
    
    return np.array(converted_times, dtype='S19')  # S19可以容纳'YYYY-MM-DD HH:MM:SS'格式


def create_output_hdf5(output_path, hdf_handler, depth_data, wse_data, flooded_area, logger, scheme_name=None):
    """
    创建符合要求的HDF5输出文件
    
    :param output_path: 输出路径
    :param hdf_handler: HDFHandler实例，用于读取HEC-RAS结果
    :param depth_data: 水深数据
    :param wse_data: 水位数据
    :param flooded_area: 淹没面积数据
    :param logger: 日志记录器
    :param scheme_name: 方案名称，用于生成文件名。如果为None，使用默认名称hydroModel.hdf5
    :return: 成功返回HDF5文件路径，失败返回None
    """
    try:
        import os
        if scheme_name:
            hdf5_output_path = os.path.join(output_path, f"{scheme_name}.hdf5")
        else:
            hdf5_output_path = os.path.join(output_path, "hydroModel.hdf5")
        
        logger.info("开始创建HDF5输出文件...")
        
        # 打开HEC-RAS结果文件读取额外数据
        import h5py
        ras_result_file = h5py.File(hdf_handler.filepath, 'r')
        
        # 读取CrossSections数据
        try:
            cross_sections_ws = ras_result_file['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['Reference Lines']['Water Surface'][:]
            cross_sections_name = ras_result_file['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['Reference Lines']['Name'][:]
            cross_sections_flow_raw = ras_result_file['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['Reference Lines']['Flow'][:]
            # 对流量取绝对值（HEC-RAS计算策略可能产生负值）
            cross_sections_flow = np.abs(cross_sections_flow_raw)
            logger.info("CrossSections数据读取成功")
        except Exception as e:
            logger.warning(f"读取CrossSections数据时出现问题: {e}")
            cross_sections_ws = None
            cross_sections_name = None
            cross_sections_flow = None
        
        # 读取TimeDateStamp
        try:
            time_date_stamp_raw = ras_result_file['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['Time Date Stamp'][:]
            time_date_stamp = convert_time_date_stamp(time_date_stamp_raw)
            logger.info("TimeDateStamp数据读取并转换成功")
        except Exception as e:
            logger.warning(f"读取TimeDateStamp数据时出现问题: {e}")
            time_date_stamp = None
        
        
        # 创建新的HDF5文件
        with h5py.File(hdf5_output_path, 'w') as f:
            # 创建根组
            data_group = f.create_group('data')
            
            # 创建2DFlowAreas组
            flow_areas_group = data_group.create_group('2DFlowAreas')
            if wse_data is not None:
                flow_areas_group.create_dataset('WaterSurface', data=wse_data)
                logger.info("2DFlowAreas/WaterSurface数据已写入")
            if depth_data is not None:
                flow_areas_group.create_dataset('depth', data=depth_data)
                logger.info("2DFlowAreas/depth数据已写入")
            if flooded_area is not None:
                flow_areas_group.create_dataset('FloodedArea', data=flooded_area)
                logger.info("2DFlowAreas/FloodedArea数据已写入")
            
            # 创建CrossSections组
            cross_sections_group = data_group.create_group('CrossSections')
            if cross_sections_ws is not None:
                cross_sections_group.create_dataset('WaterSurface', data=cross_sections_ws)
                logger.info("CrossSections/WaterSurface数据已写入")
            if cross_sections_name is not None:
                cross_sections_group.create_dataset('Name', data=cross_sections_name)
                logger.info("CrossSections/Name数据已写入")
            if cross_sections_flow is not None:
                cross_sections_group.create_dataset('Flow', data=cross_sections_flow)
                logger.info("CrossSections/Flow数据已写入")
            
            # 创建TimeDateStamp数据集
            if time_date_stamp is not None:
                data_group.create_dataset('TimeDateStamp', data=time_date_stamp)
                logger.info("TimeDateStamp数据已写入")
        
        ras_result_file.close()
        
        logger.info(f"HDF5输出文件已成功创建: {hdf5_output_path}")
        return hdf5_output_path
        
    except Exception as e:
        logger.error(f"创建HDF5输出文件时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
