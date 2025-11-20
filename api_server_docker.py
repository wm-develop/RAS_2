# -*- coding: UTF-8 -*-
import os
import shutil
import zipfile
import json
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify
from flask_cors import CORS

from to_csv import insert_time_and_save_to_csv
from hdf_handler import HDFHandler
from output_hdf_handler import create_output_hdf5
from sqlserver_handler import SQLServerHandler
from sqlserver_handler import NoArraysInDictionaryError, ArrayLengthsMismatchError, NegativeFlowError, CalInfoDataError
from post_processor import PostProcessor
from ras_handler import RASHandler
from time_format_converter import TimeFormatConverter
from velocity_to_cells import velocity_to_cells
from config import *
from logger import logger
import geopandas as gpd
import numpy as np
import pandas as pd
import threading
import requests


class NoReferenceShapefileError(Exception):
    def __init__(self, message="参考shp网格文件不存在"):
        self.message = message
        super().__init__(self.message)



class NoAreaInShapefileError(Exception):
    def __init__(self, message="参考shp网格文件中没有'Area'列"):
        self.message = message
        super().__init__(self.message)



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


def postprocess_max_water_area_shp(output_path, logger):
    try:
        import geopandas as gpd
        shp_path = os.path.join(output_path, "max_water_area.shp")
        geojson_path = os.path.join(output_path, "max_water_area_union.geojson")
        geojson_path2 = os.path.join(output_path, "max_water_area_union_simplify.geojson")

        logger.info("开始异步处理max_water_area.shp...")

        # 1. 读取shp
        gdf = gpd.read_file(shp_path)
        logger.info(f"读取shp成功，面数: {len(gdf)}")

        # 2. 转为EPSG:4326
        if gdf.crs is None or gdf.crs.to_epsg() != 4326:
            # 若没有坐标系或不是4326则转换
            gdf = gdf.to_crs(epsg=4326)
            logger.info("坐标系已转换为EPSG:4326")
        else:
            logger.info("原始shp已为EPSG:4326")

        # 3. 筛选水深>0.2的面
        # 找到 depth_* 列
        depth_col = [c for c in gdf.columns if c.startswith('depth_')]
        assert len(depth_col) == 1, f"未找到唯一的水深列，找到: {depth_col}"
        depth_col = depth_col[0]
        gdf_filtered = gdf[gdf[depth_col] > 0.2].copy()
        logger.info(f"筛选后面数: {len(gdf_filtered)}")

        if gdf_filtered.empty:
            logger.warning("筛选后无水深大于0.2的面，未生成geojson")
            return

        # 4. 融合所有面
        union_geom = gdf_filtered.unary_union
        # union_geom 可能是 Polygon 或 MultiPolygon
        out_gdf = gpd.GeoDataFrame(geometry=[union_geom], crs="EPSG:4326")

        # 5. 输出为geojson
        out_gdf.to_file(geojson_path, driver="GeoJSON")
        logger.info(f"融合后geojson已输出到 {geojson_path}")

        # ---------关键：边界简化----------
        # 你可以根据需要调整 tolerance 参数
        tolerance = 0.001  # 约100米
        union_geom_simplified = union_geom.simplify(tolerance, preserve_topology=True)
        logger.info(
            f"简化后点数：{len(union_geom_simplified.exterior.coords) if union_geom_simplified.geom_type == 'Polygon' else 'MultiPolygon'}")
        out_gdf2 = gpd.GeoDataFrame(geometry=[union_geom_simplified], crs="EPSG:4326")
        out_gdf2.to_file(geojson_path2, driver="GeoJSON")
        logger.info(f"融合并简化后geojson已输出到 {geojson_path2}")

    except Exception as e:
        logger.error(f"max_water_area.shp异步处理失败: {e}")


app = Flask(__name__)

# Enable CORS for the entire app
CORS(app)


@app.route('/set_2d_hydrodynamic_data', methods=['post'])
def set_2d_hydrodynamic_data():
    """
    api接口需要实现的具体逻辑
    :return: 失败时返回Failed+失败原因，成功时返回Success
    """
    try:
        scheme_name = request.json["scheme_name"]
        # table_out_q = request.json["table_out_q"]
        # table_out_depth = request.json["table_out_depth"]
        b01_path = os.path.join(RAS_PATH, f"FZLall.b01")
        p01_hdf_path = os.path.join(RAS_PATH, f"FZLall.p01.hdf")
        sqlserver_handler = SQLServerHandler(SQLSERVER_HOST, SQLSERVER_PORT, SQLSERVER_USER, SQLSERVER_PASSWORD,
                                             SQLSERVER_DATABASE)
        output_path = os.path.join("/root/results", scheme_name)
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        logger.info("结果输出目录已创建")
        outside_path = os.path.join(OUTPUT_PATH, scheme_name)
        logger.info("json信息解析完成")
    except Exception as e:
        logger.error(e)
        return "Failed: json信息解析失败"
    
    # 向FLOOD_REHEARSAL表中写入初始状态
    try:
        logger.info("开始写入FLOOD_REHEARSAL初始状态...")
        success = sqlserver_handler.insert_flood_rehearsal(
            flood_dispatch_name=scheme_name,
            flood_path=outside_path,
            flood_name=scheme_name,
            max_flood_area=0,
            village_info="0",
            status=0
        )
        if not success:
            logger.warning("FLOOD_REHEARSAL初始状态写入失败，但程序继续运行")
    except Exception as e:
        logger.error(f"向FLOOD_REHEARSAL写入初始状态时出错: {e}")
        return "Failed：向FLOOD_REHEARSAL写入初始状态时出错"

    try:
        ymdhm_start, ymdhm_end = sqlserver_handler.get_start_end_time(scheme_name)
    except Exception as e:
        logger.error(e)
        return "Failed：从数据库中获取模拟时段失败"

    # 以下为4个入流边界条件，即需要从数据库中读取下面4个水库的出库流量作为边界条件
    # xq_list1为白莲崖水库，xq_list2为磨子潭水库，xq_list3为佛子岭大坝，xq_list4为响洪甸水库
    try:
        xq_list = sqlserver_handler.q_from_table(scheme_name, ymdhm_start, ymdhm_end)
        xq_list1 = xq_list[:, 1]
        xq_list2 = xq_list[:, 0]
        xq_list3 = xq_list[:, 2]
        xq_list4 = xq_list[:, 3]
    except NoArraysInDictionaryError as e:
        logger.error(e)
        return "Failed: 无法查询到任何水库的出库流量过程"
    except ArrayLengthsMismatchError as e:
        logger.error(e)
        return "Failed: 数据库中各水库出库流量的时间步数不完全相同"
    except NegativeFlowError as e:
        logger.error(e)
        return "Failed: 水库出库流量序列中存在负值"
    except CalInfoDataError as e:
        logger.error(e)
        return "Failed: 响洪甸水库cal_info数据为空或数量与模拟时长不符"
    except Exception as e:
        logger.error(e)
        return "Failed: 从数据库中读取出库流量失败"

    # 修改边界条件
    try:
        ras_handler = RASHandler(xq_list1)
        time_format_converter = TimeFormatConverter()
        # 修改b01文件
        start_time_b01_and_hdf = time_format_converter.convert(
            ymdhm_start, 'b01')
        end_time_b01_and_hdf = time_format_converter.convert(ymdhm_end, 'b01')
        ras_handler.modify_b01(
            b01_path, b01_path, start_time_b01_and_hdf, end_time_b01_and_hdf)

        # 修改.p01.hdf文件，修改其中的边界条件并把Results删除后改名为.p01.tmp.hdf
        hdf_handler = HDFHandler(p01_hdf_path, ymdhm_start, ymdhm_end)
        # 修改三个入流边界、一个SA Conn边界和一个Normal Depths边界
        # hdf_handler.modify_boundary_conditions(
        #     xq_list1, xq_list2, xq_list3, start_time_b01_and_hdf, end_time_b01_and_hdf)
        hdf_handler.modify_boundary_conditions_with_xhd_hpt_rating_curve(
            xq_list1, xq_list2, xq_list3, xq_list4, start_time_b01_and_hdf, end_time_b01_and_hdf)

        # 获取符合hdf_handler.modify_plan_data方法要求的start_date和end_date，为该方法的调用做好准备
        start_time_plan_data = time_format_converter.convert(ymdhm_start, 'simulation')
        end_time_plan_data = time_format_converter.convert(ymdhm_end, 'simulation')
        # 修改p01.hdf文件中的Plan Data->Plan Information中的Simulation End Time、Simulation Start Time和Time Window
        hdf_handler.modify_plan_data(start_time_plan_data, end_time_plan_data)

        hdf_handler.remove_hdf_results()  # 得到.p01.tmp.hdf供Linux ras调用
    except Exception as e:
        logger.error(e)
        return "Failed: 修改模型边界条件时出现错误"

    # 调用run_unsteady.sh进行计算
    try:
        logger.info("开始调用HEC-RAS计算...")
        sh_path = os.path.dirname(b01_path) + os.path.sep + 'run_unsteady.sh'
        return_code = ras_handler.run_model(sh_path)
        logger.info("HEC-RAS计算完成")
    except Exception as e:
        logger.error(e)
        return "Failed: HEC-RAS计算中出现错误"

    try:
        logger.info("开始提取水位、水深数据......")
        # 从.p01.hdf结果文件中读取需要的数据
        cells_minimum_elevation_data = hdf_handler.read_dataset(
            'Cells Minimum Elevation')
        wse_data = hdf_handler.read_dataset('Water Surface')
        # velocity_u = hdf_handler.read_dataset('Velocity X')
        # velocity_v = hdf_handler.read_dataset('Velocity Y')
        # facepoints_coordinate_data = hdf_handler.read_dataset(
        #     'FacePoints Coordinate')
        # cells_coordinate_data = hdf_handler.read_dataset(
        #     'Cells Center Coordinate')
        # cells_facepoint_indexes_data = hdf_handler.read_dataset(
        #     'Cells FacePoint Indexes')
        # outflow_data = hdf_handler.read_dataset(
        #     'Outflow')

        post_processor = PostProcessor()
        # 将Cells中多余的高程为nan的空网格删去
        real_mesh = post_processor.get_real_mesh(cells_minimum_elevation_data)
        # 调用generating_depth方法，用水位减去高程，即得水深值
        depth_data, new_wse_data = post_processor.generating_depth(
            cells_minimum_elevation_data, wse_data, real_mesh)
        logger.info(f"水深和水位数据提取完成，形状: {depth_data.shape}")
        
        # # 保留原CSV输出（暂时不变）
        # csv_path = output_path + os.path.sep + "output.csv"
        # insert_time_and_save_to_csv(depth_data, csv_path)
        # logger.info(f"水深数据已写入到{csv_path}")
    except Exception as e:
        logger.error(e)
        return "Failed: 水深和水位数据提取和存储过程中出现错误"

    try:
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
    except Exception as e:
        logger.error(e)
        return f"Failed: {e}"

    # ========== 计算最大淹没面积和每个时刻的淹没面积 ==========
    try:
        flooded_area = None  # 初始化

        shp_path = RAS_PATH + os.path.sep + 'fanwei' + os.path.sep + 'fanwei.shp'
        
        if not os.path.exists(shp_path):
            logger.error(f"参考shp网格不存在: {shp_path}")
            shp_exists = False
            raise NoReferenceShapefileError(f"参考shp网格不存在: {shp_path}")
        else:
            shp_exists = True
        
        if shp_exists:
            logger.info("开始计算最大淹没面积和淹没面积...")
            
            # 读取shapefile
            gdf = gpd.read_file(shp_path)
            attributes_df = gdf
            
            # 提取面积数据 - 直接读取Area列
            if 'Area' not in attributes_df.columns:
                logger.error("Shapefile中未找到'Area'列！")
                logger.info(f"可用的列: {attributes_df.columns.tolist()}")
                raise NoAreaInShapefileError("参考Shp网格文件中未找到'Area'列！")
            
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

    except NoReferenceShapefileError as e:
        return "Failed: 参考shp网格文件不存在"
    except NoAreaInShapefileError as e:
        return "Failed: 参考shp网格文件中没有'Area'列"
        
    # 创建HDF5输出文件（使用scheme_name命名）并压缩HDF5文件为ZIP
    try:
        hdf5_file_path = create_output_hdf5(output_path, hdf_handler, depth_data, new_wse_data, flooded_area, logger, scheme_name)
        if not hdf5_file_path:
            return "Failed: HDF5输出文件创建失败"
    
    # 压缩HDF5文件为ZIP
    
        logger.info("开始压缩HDF5文件为ZIP...")
        zip_file_path = os.path.join(output_path, f"{scheme_name}.zip")
        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(hdf5_file_path, os.path.basename(hdf5_file_path))
        logger.info(f"ZIP文件已创建: {zip_file_path}")
    except Exception as e:
        logger.error(f"创建ZIP文件失败: {e}")
        return "Failed: 创建ZIP文件失败"
    
    # 写入数据库
    try:
        logger.info("开始写入数据库...")
        
        # 1. 更新FLOOD_REHEARSAL记录的STATUS为1和MAX_FLOOD_AREA
        max_flood_area = int(np.max(flooded_area))
        success = sqlserver_handler.update_flood_rehearsal_status(
            flood_dispatch_name=scheme_name,
            status=1,
            max_flood_area=max_flood_area
        )
        if not success:
            logger.warning("FLOOD_REHEARSAL状态更新失败")
        
        # 2. 准备并插入FLOOD_SECTION记录
        # 加载断面映射
        section_mapping = load_section_mapping()
        
        # 从HDF5读取断面数据
        import h5py
        with h5py.File(hdf5_file_path, 'r') as hf:
            cross_sections_ws = hf['data']['CrossSections']['WaterSurface'][:]
            cross_sections_name = hf['data']['CrossSections']['Name'][:]
            cross_sections_flow = hf['data']['CrossSections']['Flow'][:]
            time_date_stamp = hf['data']['TimeDateStamp'][:]
        
            # 准备FLOOD_SECTION批量插入数据
            section_records = []
            
            # 获取断面数据的时间步数（使用cross_sections_ws的实际列数）
            num_cross_section_timesteps = cross_sections_ws.shape[1] if len(cross_sections_ws.shape) > 1 else len(time_date_stamp)
            
            for i, cs_name in enumerate(cross_sections_name):
                # 查找映射
                if cs_name in section_mapping:
                    section_id, section_name = section_mapping[cs_name]
                    
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
        
        if section_records:
            success = sqlserver_handler.insert_flood_section_batch(section_records)
            if not success:
                logger.warning("FLOOD_SECTION批量写入失败")
        
        # 3. 准备并插入FLOODAREA记录
        floodarea_records = []
        for j in range(len(time_date_stamp)):
            time_str = time_date_stamp[j].decode('utf-8') if isinstance(time_date_stamp[j], bytes) else str(time_date_stamp[j])
            flooded_area_value = float(flooded_area[j])
            
            floodarea_records.append((
                time_str,
                flooded_area_value,
                scheme_name
            ))
        
        if floodarea_records:
            success = sqlserver_handler.insert_floodarea_batch(floodarea_records)
            if not success:
                logger.warning("FLOODAREA批量写入失败")
        
        logger.info("数据库写入完成")
        
    except Exception as e:
        logger.error(f"写入数据库时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # 数据库写入失败不影响主流程
    
    # 调用POST接口
    try:
        logger.info("开始调用POST接口...")
        
        if scheme_name:
            # 调用POST接口，上传ZIP文件
            post_url = PARSE_HOST
            zip_file_path = os.path.join(output_path, f"{scheme_name}.zip")
            
            # 检查ZIP文件是否存在
            if not os.path.exists(zip_file_path):
                logger.warning(f"ZIP文件不存在: {zip_file_path}，跳过POST接口调用")
            else:
                # 以multipart/form-data格式上传文件
                with open(zip_file_path, 'rb') as f:
                    files = {'file': (f"{scheme_name}.zip", f, 'application/zip')}
                    data = {'id': scheme_name}
                    
                    response = requests.post(post_url, files=files, data=data, timeout=30)
                    
                    if response.status_code == 200:
                        logger.info(f"POST接口调用成功: {response.text}")
                    else:
                        logger.warning(f"POST接口返回非200状态码: {response.status_code}, {response.text}")
        else:
            logger.warning("scheme_name为空，跳过POST接口调用")
            
    except Exception as e:
        logger.error(f"调用POST接口时出错: {e}")
        # POST失败不影响主流程

    # ---------------------------
    # 下面是对流速的处理
    # logger.info("开始计算每个网格的x方向流速平均值...")
    # cells_velocity_x = velocity_to_cells(
    #     cells_facepoint_indexes_data, velocity_u, real_mesh)
    # logger.info("x方向流速平均值计算完毕")
    # logger.info("开始计算每个网格的y方向流速平均值...")
    # cells_velocity_y = velocity_to_cells(
    #     cells_facepoint_indexes_data, velocity_v, real_mesh)
    # logger.info("y方向流速平均值计算完毕")
    # # 写入数据库
    # logger.info("开始将水深和流量写入数据库中")
    # # 生成水深和流量数据的时间序列
    # depth_q_time_list = time_format_converter.generate_result_timestep(ymdhm_start, ymdhm_end)
    # # 将水深和流量数据写入数据库
    # sqlserver_handler.depth_to_mysql(table_out_depth, depth_q_time_list, depth_data)
    # sqlserver_handler.q_to_mysql(table_out_q, depth_q_time_list, outflow_data)
    # logger.info("写入完成")

    # 将x和y方向的流速作为属性写入shp点文件中
    # gdal_handler = GDALHandler()
    # for i, row in enumerate(velocity_u):  # i代表第i个时间步，row代表第i个时间步下所有网格顶点的流速数据
    #     gdal_handler.write_shp(f'./velocity/point{i}.shp', facepoints_coordinate_data, row, velocity_v[i])
    #     gdal_handler.insert_raster(f'./velocity/point{i}.shp', f'./velocity/point_u{i}.tif')
    #     gdal_handler.insert_raster(f'./velocity/point{i}.shp', f'./velocity/point_v{i}.tif')
    #     # 从插值后的tif文件中读取cells中心点坐标对应栅格的像元值
    #     gdal_handler.read_tif_data(f'./velocity/point_u{i}.tif', cells_coordinate_data)
    #     gdal_handler.read_tif_data(f'./velocity/point_v{i}.tif', cells_coordinate_data)
    #
    #     if (i + 1) % 10 == 0:
    #         logger.info(f'第{i + 1}个时间步已处理完成')

    # ...主流程结束，准备异步处理max_water_area.shp
    threading.Thread(
        target=postprocess_max_water_area_shp,
        args=(output_path, logger),
        daemon=True
    ).start()
    return "success"


if __name__ == '__main__':
    # 调试时用这行代码启动服务器
    app.run(host="0.0.0.0", port=19998, debug=False)

    # 以下代码在正式生产环境用
    # server = WSGIServer(app.config["SERVER_NAME"] ,app)
    # server.serve_forever()

# if __name__ == "__main__":
#     postprocess_max_water_area_shp(r"D:\Desktop\20250415fzl\1", logger)
