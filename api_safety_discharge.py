# -*- coding: utf-8 -*-
# @Time    : 2025/3/29 下午3:31
# @Author  : wm
# @Software   : PyCharm
"""
计算x小时恒定流条件(x>=24)（100流量计算至9000，每300一步）下的指定网格是否淹没，返回淹没时的流量
"""
import os
import numpy as np
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS

from hdf_handler import HDFHandler
from post_processor import PostProcessor
from ras_handler_safety_discharge import RASHandler
from time_format_converter import TimeFormatConverter
from logger import logger
from datetime import datetime, timedelta

RAS_PATH = "/root/safety_discharge"
b01_path = os.path.join(RAS_PATH, f"FZLall.b01")
p01_hdf_path = os.path.join(RAS_PATH, f"FZLall.p01.hdf")
initial_data_path = "data.xlsx"

ymdhm_start = "2025-03-22 23:00"
start_dt = datetime.strptime(ymdhm_start, "%Y-%m-%d %H:%M")

FID = [2407, 8428, 1943, 5749, 10625]
FID_name = ["两河口", "霍山县中学", "青山乡", "下符桥镇政府", "迎驾酒厂"]
# 存储各点的安全泄量
flood_Q = {name: None for name in FID_name}


app = Flask(__name__)
CORS(app)

@app.route('/safety_discharge', methods=['post'])
def safety_discharge():
    try:
        hours = request.json['hours']
        # 类型和数值验证
        if isinstance(hours, int):
            pass  # 直接进入范围判断
        elif isinstance(hours, float):
            # 允许整数形式的浮点数（如3.0）
            if not hours.is_integer():
                raise ValueError()
            hours = int(hours)  # 转换为整数类型
        else:
            # 非数字或浮点非整数情况
            raise ValueError()

        # 范围验证
        if not (12 <= hours <= 72):
            raise ValueError()
        if hours in [2, 26, 50, 74]:
            raise ValueError()

        logger.info(f"时长数读取成功，当前传入的试算时长为：{hours}h")

    except KeyError:
        logger.error(KeyError)
        return jsonify({"error": "请求JSON中没有hours字段"}), 400
    except ValueError:
        logger.error(ValueError)
        return jsonify({"error": "hours必须为12-72之间的整数，且不能为26和50"}), 400
    except Exception as e:
        logger.error(e)
        return jsonify({"error": "请求格式不正确"}), 400

    # 根据小时数取初始流量序列
    try:
        initial_data_df = pd.read_excel(initial_data_path, sheet_name="Sheet1", header=None)
        xq_list_initial_bly = initial_data_df[0].values.ravel()  # 84小时
        xq_list_initial_mzt = initial_data_df[1].values.ravel()
        xq_list_xhd = initial_data_df[2].values.ravel()
        xq_list_initial_bly = xq_list_initial_bly[:hours + 12]
        xq_list_initial_mzt = xq_list_initial_mzt[:hours + 12]
        xq_list_xhd = xq_list_xhd[:hours + 12]
        end_dt = start_dt + timedelta(hours=hours + 11)
        ymdhm_end = end_dt.strftime("%Y-%m-%d %H:%M")
    except Exception as e:
        logger.error(e)
        return jsonify({"error": "无法序列化初始流量"}), 400

    # 开始试算
    i = 100
    while i <= 9000:
        logger.info(f"开始模拟Q = {i}的工况...")
        # 构造hours+12小时恒定流ndarray
        xq_list_fzl = np.ones(hours + 12) * i
        # 佛子岭水库的泄流过程推迟12小时，以确保水库中有足够的水量
        xq_list_fzl[:12] = 0

        # 修改白莲崖、磨子潭边界条件，响洪甸边界条件不变
        xq_list_bly = xq_list_initial_bly + i / 2
        xq_list_mzt = xq_list_initial_mzt + i / 2

        try:
            logger.info(f"将Q = {i}写入佛子岭水库边界条件中...")
            print(f"将Q = {i}写入佛子岭水库边界条件中...")
            # 修改边界条件
            ras_handler = RASHandler(xq_list_fzl)
            time_format_converter = TimeFormatConverter()
            # 修改b01文件
            start_time_b01_and_hdf = time_format_converter.convert(ymdhm_start, 'b01')
            end_time_b01_and_hdf = time_format_converter.convert(ymdhm_end, 'b01')
            ras_handler.modify_b01(b01_path, b01_path, start_time_b01_and_hdf, end_time_b01_and_hdf, '1MIN', '1HOUR')

            # 修改.p01.hdf文件，修改其中的边界条件并把Results删除后改名为.p01.tmp.hdf
            hdf_handler = HDFHandler(p01_hdf_path, ymdhm_start, ymdhm_end)
            # 修改佛子岭水库出库边界
            hdf_handler.modify_boundary_conditions_with_xhd_hpt_rating_curve(xq_list_bly, xq_list_mzt, xq_list_fzl,
                                                                             xq_list_xhd, start_time_b01_and_hdf,
                                                                             end_time_b01_and_hdf)

            # 获取符合hdf_handler.modify_plan_data方法要求的start_date和end_date，为该方法的调用做好准备
            start_time_plan_data = time_format_converter.convert(ymdhm_start, 'simulation')
            end_time_plan_data = time_format_converter.convert(ymdhm_end, 'simulation')
            # 修改p01.hdf文件中的Plan Data->Plan Information中的Simulation End Time、Simulation Start Time和Time Window
            hdf_handler.modify_plan_data(start_time_plan_data, end_time_plan_data)

            # 得到.p01.tmp.hdf供Linux ras调用
            hdf_handler.remove_hdf_results()
        except Exception as e:
            logger.error(e)
            return jsonify({"error": "修改模型边界条件时出现错误"}), 400

        # 调用run_unsteady.sh进行计算
        try:
            logger.info("开始调用HEC-RAS计算...")
            sh_path = os.path.dirname(b01_path) + os.path.sep + 'run_unsteady.sh'
            return_code = ras_handler.run_model(sh_path)
            logger.info("HEC-RAS计算完成")
            print("HEC-RAS计算完成")
        except Exception as e:
            logger.error(e)
            return jsonify({"error": "HEC-RAS计算中出现错误"}), 400

        try:
            logger.info("开始提取全部网格的水深数据......")
            # 从.p01.hdf结果文件中读取需要的数据
            cells_minimum_elevation_data = hdf_handler.read_dataset(
                'Cells Minimum Elevation')
            wse_data = hdf_handler.read_dataset('Water Surface')

            post_processor = PostProcessor()
            # 将Cells中多余的高程为nan的空网格删去
            real_mesh = post_processor.get_real_mesh(cells_minimum_elevation_data)
            # 调用generating_depth方法，用水位减去高程，即得水深值
            depth_data = post_processor.generating_depth(
                cells_minimum_elevation_data, wse_data, real_mesh)
            logger.info("全部网格的水深数据已提取完成")
            logger.info(f"Q = {i}条件下，depth_data中总共含有{depth_data.shape[0]}行数据")
        except Exception as e:
            logger.error(e)
            return jsonify({"error": "水深数据提取过程中出现错误"}), 400


        if depth_data.shape[0] != (hours + 12) * 6 - 5:
            # 此时，模型计算发散，需要丢弃此次数据，稍微改变流量过程重新试算
            logger.info(f"Q = {i}条件下，模型计算不收敛，将丢弃此次结果，采用较小步长重新试算...")
            i += 100
            continue

        try:
            logger.info("开始提取关注点的水深数据...")
            # 提取指定位置的水深时序数据
            depth_data_FID = depth_data[:, FID]
            # 判断每个位置是否淹没
            is_drown = (depth_data_FID >= 0.2).any(axis=0)
            for j, drown in enumerate(is_drown):
                if drown:
                    if flood_Q[FID_name[j]] is None:
                        logger.info(f"Q = {i}条件下，{FID_name[j]} 已经计算出不为0的淹没水深...")
                        flood_Q[FID_name[j]] = i
        except Exception as e:
            logger.error(e)
            return jsonify({"error": "处理关注点的淹没判断逻辑时出现错误"}), 400

        i += 300

        # 判断是否已经得到所有的安全泄量
        if all(value is not None for value in flood_Q.values()):
            logger.info("已试算得到所有关注点的安全泄量，程序提前终止...")
            break

        logger.info("------------------------")

    return jsonify(flood_Q), 200


if __name__ == '__main__':
    # 调试时用这行代码启动服务器
    app.run(host="0.0.0.0", port=19997, debug=False)
