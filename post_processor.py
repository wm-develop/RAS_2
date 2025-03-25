import numpy as np
import pandas as pd
from logger import logger
# import shapefile


class PostProcessor:
    def __init__(self):
        pass

    def generating_depth(self, dem_data: np.ndarray, wse_data: np.ndarray, real_mesh):
        """
        用水位数据减去高程数据得到水深数据
        :param dem_data:
        :param wse_data:
        :return: 返回水深数据，类型为list，[[时间步1的每个cells的水深],[时间步2的每个cells的水深],...]
        """
        new_dem_data = dem_data[:real_mesh]
        new_wse_data = wse_data[:, :real_mesh]
        depth_data = list()  # 存放所有时间步长下所有网格的水深值
        for row in new_wse_data:  # 每个row代表一个时间步
            depth_data.append((row - new_dem_data).tolist())
        depth_data_array = np.array(depth_data)
        return depth_data_array

    def get_real_mesh(self, dem_data):
        for cells_number, data in enumerate(dem_data):
            data = str(data)
            if data == 'nan':
                break
        return cells_number

    def get_water_level(self, wse_data, real_mesh):
        """
        返回记录水位的二维数组，行代表时间步，列代表网格FID
        """
        return wse_data[:, :real_mesh]

    def calculate_and_save_row_means(self, water_level_array, output_file_path, column_indices):
        """
        从water_level_array中提取指定列并计算每行的平均值，然后将结果保存到新的CSV文件中。

        参数:
        - water_level_array: 保存水位的二维数组。行代表时间步，列代表网格FID。
        - output_file_path: 输出 CSV 文件的路径。
        - column_indices: 需要提取的列索引列表（0开始索引）。
        """
        try:
            # 读取指定的列，读取所有行
            data = water_level_array[:, column_indices]

            # 计算每行的平均值
            row_means = np.mean(data, axis=1)

            # 将结果转换为 DataFrame
            result_df = pd.DataFrame(row_means)

            # 保存结果到新的 CSV 文件，不包含行索引和列名
            result_df.to_csv(output_file_path, index=False, header=False)

            logger.info(f"Row means have been saved to {output_file_path}.")

        except IndexError as e:
            logger.error("Failed: 提取坝下流量过程中出现错误：列索引越界")
            return 1
        except Exception as e:
            logger.error("Failed: 提取坝下流量过程中出现未知错误")
            return 1
