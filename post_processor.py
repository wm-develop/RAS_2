import numpy as np
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
