# -*- coding: UTF-8 -*-
"""
HDF5输出功能测试脚本
跳过模型计算前的所有逻辑，直接从HEC-RAS结果文件读取数据并生成HDF5输出
"""
import os
import numpy as np
import geopandas as gpd
from hdf_handler import HDFHandler
from output_hdf_handler import create_output_hdf5
from post_processor import PostProcessor
from to_csv import insert_time_and_save_to_csv
from logger import logger


def test_hdf5_output():
    """测试HDF5输出功能"""
    
    # ========== 配置参数 ==========
    # HEC-RAS结果文件路径
    p01_hdf_path = r"D:\Desktop\fzl_history\Fzlmodel20251030test\FZLall.p01.hdf"
    
    # 输出路径
    output_path = r"D:\Desktop\fzl_history\test_output"
    
    # Shapefile路径（用于计算淹没面积）
    # 请根据实际情况修改这个路径
    shp_path = r"D:\Desktop\fzl_history\Fzlmodel20251030test\fanwei\fanwei.shp"
    
    # 创建输出目录
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        logger.info(f"创建输出目录: {output_path}")
    
    # ========== 初始化 ==========
    logger.info("="*60)
    logger.info("开始测试HDF5输出功能")
    logger.info(f"HEC-RAS结果文件: {p01_hdf_path}")
    logger.info(f"输出路径: {output_path}")
    logger.info("="*60)
    
    # 检查文件是否存在
    if not os.path.exists(p01_hdf_path):
        logger.error(f"HEC-RAS结果文件不存在: {p01_hdf_path}")
        return False
    
    if not os.path.exists(shp_path):
        logger.warning(f"Shapefile不存在: {shp_path}")
        logger.warning("将跳过淹没面积计算，FloodedArea数据集将为空")
        shp_exists = False
    else:
        shp_exists = True
    
    try:
        # ========== 读取HEC-RAS结果数据 ==========
        logger.info("开始读取HEC-RAS结果数据...")
        
        # 创建HDFHandler实例（ymdhm参数设为None，因为我们不需要修改边界条件）
        hdf_handler = HDFHandler(p01_hdf_path, None, None)
        
        # 读取需要的数据
        cells_minimum_elevation_data = hdf_handler.read_dataset('Cells Minimum Elevation')
        wse_data = hdf_handler.read_dataset('Water Surface')
        logger.info("HEC-RAS结果数据读取完成")
        
        # ========== 处理水深数据 ==========
        logger.info("开始处理水深数据...")
        post_processor = PostProcessor()
        
        # 将Cells中多余的高程为nan的空网格删去
        real_mesh = post_processor.get_real_mesh(cells_minimum_elevation_data)
        
        # 调用generating_depth方法，用水位减去高程，即得水深值
        depth_data, new_wse_data = post_processor.generating_depth(
            cells_minimum_elevation_data, wse_data, real_mesh)
        logger.info(f"水深数据提取完成，形状: {depth_data.shape}")
        
        # # ========== 保存CSV（可选）==========
        # logger.info("开始保存CSV文件...")
        # csv_path = os.path.join(output_path, "output.csv")
        # insert_time_and_save_to_csv(depth_data, csv_path)
        # logger.info(f"CSV文件已保存: {csv_path}")
        
        # ========== 提取坝下水位 ==========
        logger.info("开始提取坝下水位...")
        bailianya_grids = [25495, 25494, 25496, 25492]
        mozitan_grids = [24834, 24833, 24835, 24832]
        foziling_grids = [24418, 24417, 17369, 17367]
        
        water_level_array = post_processor.get_water_level(wse_data, real_mesh)
        
        bailianya_dam_depth_path = os.path.join(output_path, "bailianya.csv")
        mozitan_dam_depth_path = os.path.join(output_path, "mozitan.csv")
        foziling_dam_depth_path = os.path.join(output_path, "foziling.csv")
        
        post_processor.calculate_and_save_row_means(
            water_level_array, bailianya_dam_depth_path, bailianya_grids)
        post_processor.calculate_and_save_row_means(
            water_level_array, mozitan_dam_depth_path, mozitan_grids)
        post_processor.calculate_and_save_row_means(
            water_level_array, foziling_dam_depth_path, foziling_grids)
        logger.info("坝下水位提取完成")
        
        # ========== 计算最大淹没面积和每个时刻的淹没面积 ==========
        flooded_area = None  # 初始化
        
        if shp_exists:
            logger.info("开始计算最大淹没面积和淹没面积...")
            
            # 读取shapefile
            gdf = gpd.read_file(shp_path)
            attributes_df = gdf
            
            # 提取面积数据 - 直接读取Area列
            if 'Area' not in attributes_df.columns:
                logger.error("Shapefile中未找到'Area'列！")
                logger.info(f"可用的列: {attributes_df.columns.tolist()}")
                return False
            
            area_data = attributes_df[['Area']].to_numpy()
            
            depth_data_transposed = depth_data.T
            # depth_data_final: 行=网格FID，列=时间步
            # 第一列为面积，后续列为各时间步的水深
            depth_data_final = np.hstack((area_data, depth_data_transposed))
            
            num_shapefiles = len(depth_data)
            logger.info(f"时间步数: {num_shapefiles}")
            
            # 计算每个网格在每个时间步的受淹面积
            depth_count = np.zeros((len(depth_data_final), num_shapefiles))
            for i in range(len(depth_data_final)):
                for j in range(num_shapefiles):
                    x = depth_data_final[i, j + 1]  # 获取水深值
                    if x > 0.2:  # 水深>0.2m
                        depth_count[i][j] = depth_data_final[i][0] * 0.001 * 0.001  # 转换为km²
                    else:
                        depth_count[i][j] = 0
            
            depth_count_final = depth_count.sum(axis=0)
            max_index = np.argmax(depth_count_final)
            
            # 添加最大淹没时刻的水深到shapefile
            attributes_df[f'depth_{max_index}'] = depth_data_final[:, max_index + 1]
            
            # 保存最大淹没面积shapefile
            max_shp_path = os.path.join(output_path, "max_water_area.shp")
            gdf.to_file(max_shp_path)
            logger.info(f"最大淹没面积shp文件已保存: {max_shp_path}")
            logger.info(f"最大淹没发生在第{max_index}个时间步")
            
            # ========== 计算每个时刻的淹没面积 ==========
            logger.info("开始计算每个时刻的淹没面积...")
            grid_areas = attributes_df['Area'].values
            flooded_area = np.zeros(num_shapefiles)
            
            for j in range(num_shapefiles):
                total_area = 0.0
                for i in range(len(depth_data_final)):
                    depth = depth_data_final[i, j + 1]
                    if depth > 0.2:
                        total_area += grid_areas[i]  # 累加面积(m²)
                
                # 减去42756184m²，转换为km²
                flooded_area_km2 = (total_area - 42756184.0) / 1000000.0
                flooded_area[j] = max(0.0, flooded_area_km2)
            
            logger.info(f"淹没面积计算完成，共{num_shapefiles}个时间步")
            logger.info(f"淹没面积范围: {flooded_area.min():.2f} - {flooded_area.max():.2f} km²")
        
        # ========== 创建HDF5输出文件 ==========
        logger.info("开始创建HDF5输出文件...")
        hdf5_success = create_output_hdf5(
            output_path, hdf_handler, depth_data, new_wse_data, flooded_area, logger)
        
        if hdf5_success:
            logger.info("="*60)
            logger.info("测试成功完成！")
            logger.info(f"HDF5输出文件: {os.path.join(output_path, 'hydroModel.hdf5')}")
            logger.info("="*60)
            return True
        else:
            logger.error("HDF5文件创建失败")
            return False
            
    except Exception as e:
        logger.error(f"测试过程中出现错误: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    # 运行测试
    success = test_hdf5_output()
    
    if success:
        print("\n" + "="*60)
        print("测试成功！请检查输出目录中的 hydroModel.hdf5 文件")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("测试失败！请查看日志了解详细错误信息")
        print("="*60)
