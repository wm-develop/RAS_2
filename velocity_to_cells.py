from hdf_handler import HDFHandler
from post_processor import PostProcessor

from logger import logger


def velocity_to_cells(cells_facepoint_indexes_data, velocity_data, cells_number):
    cells_velocity = []
    # velocity_row代表每个时间步下的facepoint流速数据
    for time_step, velocity_row in enumerate(velocity_data):
        cells_velocity_perstep = []  # 存储每一步的每个cells的流速
        # cell_row代表每个cell的facepoint index数据
        for cell_index, cell_row in enumerate(cells_facepoint_indexes_data):
            if cell_index == cells_number:
                break
            facepoint_velocity_sum = 0  # 用于记录每个cells网格顶点的流速和
            flag = True  # flag用于判断循环是正常结束还是被打断
            # facepoint_index
            for index, facepoint_index in enumerate(cell_row):
                if facepoint_index == -1:  # facepoint_index
                    flag = False
                    break
                facepoint_velocity_sum += velocity_row[facepoint_index]
            if flag:
                cell_velocity = facepoint_velocity_sum / (index + 1)
            else:
                cell_velocity = facepoint_velocity_sum / index

            cells_velocity_perstep.append(cell_velocity)

        cells_velocity.append(cells_velocity_perstep)
        logger.info(f'已处理{time_step + 1}个时间步')
    return cells_velocity


if __name__ == '__main__':
    hdf_handler = HDFHandler(r'D:\Desktop\ras_shangyehe_02\syh.p01.hdf')
    velocity_x_data = hdf_handler.read_dataset('Velocity X')
    velocity_y_data = hdf_handler.read_dataset('Velocity Y')
    cells_facepoint_indexes_data = hdf_handler.read_dataset(
        'Cells FacePoint Indexes')
    dem_data = hdf_handler.read_dataset('Cells Minimum Elevation')
    post_processor = PostProcessor()
    cells_number = post_processor.get_real_mesh(dem_data)

    cells_velocity_x = velocity_to_cells(
        cells_facepoint_indexes_data, velocity_x_data, cells_number)
    cells_velocity_y = velocity_to_cells(
        cells_facepoint_indexes_data, velocity_y_data, cells_number)
