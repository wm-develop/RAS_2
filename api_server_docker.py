# -*- coding: UTF-8 -*-
import os
import shutil
from concurrent.futures import ThreadPoolExecutor

from flask import Flask, request, jsonify
from flask_cors import CORS

from to_csv import insert_time_and_save_to_csv
from hdf_handler import HDFHandler
from sqlserver_handler import SQLServerHandler
from sqlserver_handler import NoArraysInDictionaryError
from sqlserver_handler import ArrayLengthsMismatchError
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
        sqlserver_handler = SQLServerHandler(SQLSERVER_HOST, SQLSERVER_PORT, SQLSERVER_USER, SQLSERVER_PASSWORD, SQLSERVER_DATABASE)
        logger.info("json信息解析完成")
    except Exception as e:
        logger.error(e)
        return "Failed: json信息解析失败"

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
        logger.info("开始将水深数据存储到csv文件中......")
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
        depth_data = post_processor.generating_depth(
            cells_minimum_elevation_data, wse_data, real_mesh)
        logger.info("水深数据提取已完成")

        output_path = "/root/results"
        csv_path = output_path + os.path.sep + "output.csv"
        # 将水深计算结果输出为csv文件
        insert_time_and_save_to_csv(depth_data, csv_path)
        logger.info(f"水深数据已写入到{csv_path}")
    except Exception as e:
        logger.error(e)
        return "Failed: 水深数据提取和存储过程中出现错误"

    try:
        logger.info("正在提取坝下水位过程...")
        bailianya_dam_depth_path = output_path + os.path.sep + "bailianya.csv"
        mozitan_dam_depth_path = output_path + os.path.sep + "mozitan.csv"
        foziling_dam_depth_path = output_path + os.path.sep + "foziling.csv"
        bailianya_grids = [25495, 25494, 25496, 25492]
        mozitan_grids = [24834, 24833, 24835, 24832]
        foziling_grids = [24418, 24417, 17369, 17367]
        water_level_array = post_processor.get_water_level(wse_data, real_mesh)
        bailianya_dam_depth_result = post_processor.calculate_and_save_row_means(water_level_array, bailianya_dam_depth_path, bailianya_grids)
        mozitan_dam_depth_result = post_processor.calculate_and_save_row_means(water_level_array, mozitan_dam_depth_path, mozitan_grids)
        foziling_dam_depth_result = post_processor.calculate_and_save_row_means(water_level_array, foziling_dam_depth_path, foziling_grids)
        if bailianya_dam_depth_result == 1 or mozitan_dam_depth_result == 1 or foziling_dam_depth_result == 1:
            raise RuntimeError("Failed: 提取坝下水位过程中出现错误")
        logger.info("坝下水位过程提取成功")
    except Exception as e:
        logger.error(e)
        return f"Failed: {e}"

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
            return "Failed: 最大淹没面积shp文件被占用，无法写入"
        return "Failed: 计算最大淹没面积时出现错误"

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
        logger.info(f"简化后点数：{len(union_geom_simplified.exterior.coords) if union_geom_simplified.geom_type=='Polygon' else 'MultiPolygon'}")
        out_gdf2 = gpd.GeoDataFrame(geometry=[union_geom_simplified], crs="EPSG:4326")
        out_gdf2.to_file(geojson_path2, driver="GeoJSON")
        logger.info(f"融合并简化后geojson已输出到 {geojson_path2}")

    except Exception as e:
        logger.error(f"max_water_area.shp异步处理失败: {e}")


# if __name__ == "__main__":
#     postprocess_max_water_area_shp(r"D:\Desktop\20250415fzl\1", logger)
