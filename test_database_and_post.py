# -*- coding: UTF-8 -*-
"""
测试数据库写入和POST接口调用的独立脚本
用于调试从HDF5读取数据并写入数据库、调用POST接口的逻辑
"""
import os
import h5py
import numpy as np
import pandas as pd
import requests
from sqlserver_handler import SQLServerHandler
from config import *
from logger import logger


def load_section_mapping():
    """
    加载断面ID和名称的映射关系
    从断面id名称对应关系.xlsx文件中读取映射
    
    :return: 字典，key为cross_sections_name（字节串），value为(SECTION_ID, SECTION_NAME)元组
    """
    try:
        import pandas as pd
        import os
        
        # 断面映射文件路径（相对于当前工作目录）
        mapping_file = os.path.join(os.path.dirname(__file__), "断面id名称对应关系.xlsx")
        
        if not os.path.exists(mapping_file):
            logger.warning(f"断面映射文件不存在: {mapping_file}")
            return {}
        
        # 读取Excel文件
        df = pd.read_excel(mapping_file, sheet_name='FLOODAREA')
        
        # 建立映射字典
        section_map = {}
        for _, row in df.iterrows():
            cross_sections_id = row['cross_sections_id']
            # 跳过cross_sections_id为"无"的记录
            if pd.isna(cross_sections_id) or str(cross_sections_id).strip() == "无":
                continue
            
            cross_sections_name = row['cross_sections_name']
            section_id = int(row['SECTION_ID'])
            section_name = str(row['SECTION_NAME'])
            
            # 将cross_sections_name转为字节串作为key（与HDF5中的格式一致）
            key = cross_sections_name.encode('utf-8') if isinstance(cross_sections_name, str) else cross_sections_name
            section_map[key] = (section_id, section_name)
        
        logger.info(f"成功加载{len(section_map)}个断面映射")
        return section_map
        
    except Exception as e:
        logger.error(f"加载断面映射失败: {e}")
        return {}


def test_database_and_post():
    """
    测试数据库写入和POST接口调用
    """
    # ========== 配置部分 - 请修改这些参数 ==========
    scheme_name = "小流量调度方案仿真"  # 方案名称
    hdf5_file_path = "/root/results/小流量调度方案仿真/小流量调度方案仿真.hdf5"  # HDF5文件路径
    output_path = "/root/results/小流量调度方案仿真"  # 输出路径
    
    # ========== 初始化数据库连接 ==========
    logger.info("初始化数据库连接...")
    sqlserver_handler = SQLServerHandler(
        SQLSERVER_HOST, 
        SQLSERVER_PORT, 
        SQLSERVER_USER, 
        SQLSERVER_PASSWORD,
        SQLSERVER_DATABASE
    )
    
    # ========== 读取HDF5文件 ==========
    try:
        logger.info(f"开始读取HDF5文件: {hdf5_file_path}")
        
        if not os.path.exists(hdf5_file_path):
            logger.error(f"HDF5文件不存在: {hdf5_file_path}")
            return
        
        with h5py.File(hdf5_file_path, 'r') as hf:
            # 读取断面数据
            cross_sections_ws = hf['data']['CrossSections']['WaterSurface'][:]
            cross_sections_name = hf['data']['CrossSections']['Name'][:]
            cross_sections_flow = hf['data']['CrossSections']['Flow'][:]
            time_date_stamp = hf['data']['TimeDateStamp'][:]
            
            # 读取淹没面积数据
            flooded_area = hf['data']['2DFlowAreas']['FloodedArea'][:]
            
            logger.info(f"断面数量: {len(cross_sections_name)}")
            logger.info(f"时间步数(time_date_stamp): {len(time_date_stamp)}")
            logger.info(f"断面数据形状(WaterSurface): {cross_sections_ws.shape}")
            logger.info(f"淹没面积数据长度: {len(flooded_area)}")
    
    except Exception as e:
        logger.error(f"读取HDF5文件失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return
    
    # ========== 写入数据库 ==========
    try:
        logger.info("开始写入数据库...")
        
        # 1. 更新FLOOD_REHEARSAL记录的STATUS为1和MAX_FLOOD_AREA
        max_flood_area = int(np.max(flooded_area))
        logger.info(f"最大淹没面积: {max_flood_area} km²")
        
        success = sqlserver_handler.update_flood_rehearsal_status(
            flood_dispatch_name=scheme_name,
            status=1,
            max_flood_area=max_flood_area
        )
        if not success:
            logger.warning("FLOOD_REHEARSAL状态更新失败")
        
        # 2. 准备并插入FLOOD_SECTION记录
        logger.info("开始准备FLOOD_SECTION数据...")
        
        # 加载断面映射
        section_mapping = load_section_mapping()
        
        # 获取断面数据的时间步数（使用cross_sections_ws的实际列数）
        num_cross_section_timesteps = cross_sections_ws.shape[1] if len(cross_sections_ws.shape) > 1 else len(time_date_stamp)
        logger.info(f"断面数据实际时间步数: {num_cross_section_timesteps}")
        
        # 准备FLOOD_SECTION批量插入数据
        section_records = []
        for i, cs_name in enumerate(cross_sections_name):
            # 查找映射
            if cs_name in section_mapping:
                section_id, section_name = section_mapping[cs_name]
                logger.info(f"处理断面 [{i}] {section_name} (ID: {section_id})")
                
                # 为每个时间步创建一条记录（使用断面数据实际的时间步数）
                for j in range(num_cross_section_timesteps):
                    time_str = time_date_stamp[j].decode('utf-8') if isinstance(time_date_stamp[j], bytes) else str(time_date_stamp[j])
                    z_value = float(cross_sections_ws[i, j])
                    q_value = float(cross_sections_flow[i, j])
                    depth_value = 0  # DEPTH字段暂填0
                    
                    section_records.append((
                        section_id,
                        section_name,
                        scheme_name,
                        time_str,
                        z_value,
                        depth_value,
                        q_value
                    ))
            else:
                cs_name_str = cs_name.decode('utf-8') if isinstance(cs_name, bytes) else str(cs_name)
                logger.warning(f"断面 [{i}] {cs_name_str} 未找到映射，跳过")
        
        logger.info(f"准备插入{len(section_records)}条FLOOD_SECTION记录")
        
        if section_records:
            success = sqlserver_handler.insert_flood_section_batch(section_records)
            if not success:
                logger.error("FLOOD_SECTION批量写入失败")
            else:
                logger.info("FLOOD_SECTION批量写入成功")
        
        # 3. 准备并插入FLOODAREA记录
        logger.info("开始准备FLOODAREA数据...")
        floodarea_records = []
        for j in range(len(time_date_stamp)):
            time_str = time_date_stamp[j].decode('utf-8') if isinstance(time_date_stamp[j], bytes) else str(time_date_stamp[j])
            flooded_area_value = float(flooded_area[j])
            
            floodarea_records.append((
                time_str,
                flooded_area_value,
                scheme_name
            ))
        
        logger.info(f"准备插入{len(floodarea_records)}条FLOODAREA记录")
        
        if floodarea_records:
            success = sqlserver_handler.insert_floodarea_batch(floodarea_records)
            if not success:
                logger.error("FLOODAREA批量写入失败")
            else:
                logger.info("FLOODAREA批量写入成功")
        
        logger.info("数据库写入完成")
        
    except Exception as e:
        logger.error(f"写入数据库时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    # ========== 调用POST接口 ==========
    try:
        logger.info("开始调用POST接口...")
        
        if scheme_name:
            # 调用POST接口，上传ZIP文件
            post_url = PARSE_HOST
            # 注意：这里需要使用实际的ZIP文件路径
            # 测试时请根据实际情况修改ZIP文件路径
            zip_file_path = os.path.join(output_path, f"{scheme_name}.zip")
            
            logger.info(f"POST URL: {post_url}")
            logger.info(f"ZIP文件路径: {zip_file_path}")
            
            # 检查ZIP文件是否存在
            if not os.path.exists(zip_file_path):
                logger.warning(f"ZIP文件不存在: {zip_file_path}，跳过POST接口调用")
            else:
                # 以multipart/form-data格式上传文件
                with open(zip_file_path, 'rb') as f:
                    files = {'file': (f"{scheme_name}.zip", f, 'application/zip')}
                    data = {'id': scheme_name}
                    
                    logger.info(f"上传文件: {scheme_name}.zip, ID: {scheme_name}")
                    
                    response = requests.post(post_url, files=files, data=data, timeout=30)
                    
                    logger.info(f"响应状态码: {response.status_code}")
                    logger.info(f"响应内容: {response.text}")
                    
                    if response.status_code == 200:
                        logger.info("POST接口调用成功")
                    else:
                        logger.warning(f"POST接口返回非200状态码")
        else:
            logger.warning("scheme_name为空，跳过POST接口调用")
            
    except Exception as e:
        logger.error(f"调用POST接口时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("开始测试数据库写入和POST接口调用")
    logger.info("=" * 60)
    
    test_database_and_post()
    
    logger.info("=" * 60)
    logger.info("测试完成")
    logger.info("=" * 60)
