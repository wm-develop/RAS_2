# -*- coding: UTF-8 -*-
import os
import time
import shutil
import h5py
import numpy as np
import pandas as pd


class HDFHandler:
    def __init__(self, filepath: str, ymdhm_start, ymdhm_end):
        self.filepath = filepath
        self.ymdhm_start = ymdhm_start
        self.ymdhm_end = ymdhm_end

    def remove_hdf_results(self):
        """
        Linux版本的ras要求提供一份windows版本运行的.p01.hdf结果文件，但是需要将文件中的Results组删除，本方法实现上述功能
        输入一个.p01.hdf文件，输出一个删除Results后的.p01.tmp.hdf文件供Linux ras计算引擎调用
        :return: 无返回值
        """
        os.popen(f"python remove_HDF5_Results.py {self.filepath}")
        time.sleep(3)

        # 把新生成的.p01.tmp.hdf移动到./wrk_source文件夹下
        if not os.path.exists(os.path.dirname(self.filepath) + os.path.sep + 'wrk_source'):
            os.mkdir(os.path.dirname(self.filepath) + os.path.sep + 'wrk_source')

        shutil.move(os.path.splitext(self.filepath)[0] + '.tmp.hdf',
                    os.path.dirname(self.filepath) + os.path.sep + 'wrk_source' + os.path.sep +
                    os.path.basename(self.filepath).split('.')[0] + '.p01.tmp.hdf')

    def modify_plan_data(self, start_date, end_date):
        """
        修改Plan Data->Plan Information中的Simulation End Time、Simulation Start Time和Time Window
        :param start_date: 开始时间，格式为DDMmmYYYY HH:MM:SS
        :param end_date: 结束时间，格式为DDMmmYYYY HH:MM:SS
        """
        f = h5py.File(self.filepath, 'a')
        time_window = start_date + ' to ' + end_date

        plan_info = f['Plan Data']['Plan Information']

        # 记录原始属性
        boi_data = plan_info.attrs['Base Output Interval']
        ctsb_data = plan_info.attrs['Computation Time Step Base']
        ff_data = plan_info.attrs['Flow Filename']
        ft_data = plan_info.attrs['Flow Title']
        gf_data = plan_info.attrs['Geometry Filename']
        gt_data = plan_info.attrs['Geometry Title']
        pf_data = plan_info.attrs['Plan Filename']
        pn_data = plan_info.attrs['Plan Name']
        psid_data = plan_info.attrs['Plan ShortID']
        pt_data = plan_info.attrs['Plan Title']
        set_data = plan_info.attrs['Simulation End Time']
        sst_data = plan_info.attrs['Simulation Start Time']
        tw_data = plan_info.attrs['Time Window']

        # 删除旧的 group
        del f['/Plan Data/Plan Information']

        # 创建新的 group
        new_plan_info = f['Plan Data'].create_group('Plan Information')

        # 恢复原来的属性，并修改开始和结束时间
        # 在新的group中按照记录下来的属性重新新建
        new_plan_info.attrs.create('Base Output Interval', boi_data, dtype=boi_data.dtype)
        new_plan_info.attrs.create('Computation Time Step Base', ctsb_data, dtype=ctsb_data.dtype)
        new_plan_info.attrs.create('Flow Filename', ff_data, dtype=gt_data.dtype)
        new_plan_info.attrs.create('Flow Title', ft_data, dtype=ft_data.dtype)
        new_plan_info.attrs.create('Geometry Filename', gf_data, dtype=gt_data.dtype)
        new_plan_info.attrs.create('Geometry Title', gt_data, dtype=pf_data.dtype)
        new_plan_info.attrs.create('Plan Filename', pf_data, dtype=pn_data.dtype)
        new_plan_info.attrs.create('Plan Name', pn_data, dtype=psid_data.dtype)
        new_plan_info.attrs.create('Plan ShortID', psid_data, dtype=pt_data.dtype)
        new_plan_info.attrs.create('Plan Title', pt_data, dtype=pt_data.dtype)

        new_plan_info.attrs.create('Simulation Start Time', start_date, dtype=sst_data.dtype)
        new_plan_info.attrs.create('Simulation End Time', end_date, dtype=set_data.dtype)
        new_plan_info.attrs.create('Time Window', time_window, dtype=tw_data.dtype)

    def modify_boundary_conditions(self, qc_list1, qc_list2, qc_list3, start_date, end_date):
        f = h5py.File(self.filepath, 'a')

        # 使用通用函数处理多个边界条件
        # 修改两个入流的边界条件
        self._modify_bc(f, qc_list1, start_date, end_date, '2D: Perimeter 1 BCLine: Bailianya Inflow')
        self._modify_bc(f, qc_list2, start_date, end_date, '2D: Perimeter 1 BCLine: Mozitan Inflow')
        # 虽然响洪甸不能从数据库中读取出库流量，但需要保证它的时序数量为start_date到end_date
        self._modify_xianghongdian(f, start_date, end_date)

        # 修改SA Conn的边界条件
        self._modify_sa_conn(f, qc_list3, start_date, end_date, 'SA Conn: Foziling Dam (Outlet TS: Foziling Boundar)')
        # 修改Normal Depth出流的起始时间
        self._modify_normal_depths(f, start_date, end_date, '2D: Perimeter 1 BCLine: Hengpaitou Outflow')

        f.close()

    def _modify_sa_conn(self, f, qc_list, start_date, end_date, dataset_name):
        """
        修改SA Conn边界条件的函数
        :param f: h5py 文件对象
        :param qc_list: 边界条件的流量数据列表
        :param start_date: 模拟开始时间
        :param end_date: 模拟结束时间
        :param dataset_name: hdf5 中的 dataset 路径
        """
        sa_dataset = f['Event Conditions']['Unsteady']['Boundary Conditions']['Flow Hydrographs'][dataset_name]

        # 转换流量列表为浮点数并生成时间数组
        qc_list_float = list()
        for data in qc_list:
            qc_list_float.append(float(data))

        sa_data = np.array(qc_list_float)
        time_list = list()
        for i in range(len(qc_list_float)):
            time_list.append(i / 24)
        time_array = np.array(time_list)
        sa_data = np.column_stack((time_array, sa_data))

        # 记录原始属性
        conn_data = sa_dataset.attrs['Connection']
        dt_data = sa_dataset.attrs['Data Type']
        ed_data = sa_dataset.attrs['End Date']
        i_data = sa_dataset.attrs['Interval']
        ni_data = sa_dataset.attrs['Node Index']
        sd_data = sa_dataset.attrs['Start Date']

        # 删除旧的 dataset
        del f['Event Conditions']['Unsteady']['Boundary Conditions']['Flow Hydrographs'][dataset_name]

        # 创建新的 dataset
        new_bc_dataset = f['Event Conditions']['Unsteady']['Boundary Conditions']['Flow Hydrographs'].create_dataset(
            dataset_name, data=sa_data)

        # 恢复原来的属性，并修改开始和结束时间
        # 在新的Dataset中按照记录下来的属性重新新建
        new_bc_dataset.attrs.create('Connection', conn_data, dtype=conn_data.dtype)
        new_bc_dataset.attrs.create('Data Type', dt_data, dtype=dt_data.dtype)
        new_bc_dataset.attrs.create('Interval', i_data, dtype=i_data.dtype)
        new_bc_dataset.attrs.create('Node Index', ni_data, dtype=ni_data.dtype)

        new_bc_dataset.attrs.create('End Date', end_date, dtype=ed_data.dtype)
        new_bc_dataset.attrs.create('Start Date', start_date, dtype=sd_data.dtype)

    def _modify_bc(self, f, qc_list, start_date, end_date, dataset_name):
        """
        通用的修改边界条件的函数
        :param f: h5py 文件对象
        :param qc_list: 边界条件的流量数据列表
        :param start_date: 模拟开始时间
        :param end_date: 模拟结束时间
        :param dataset_name: hdf5 中的 dataset 路径
        """
        bc_dataset = f['Event Conditions']['Unsteady']['Boundary Conditions']['Flow Hydrographs'][dataset_name]

        # 转换流量列表为浮点数并生成时间数组
        qc_list_float = list()
        for data in qc_list:
            qc_list_float.append(float(data))

        bc_data = np.array(qc_list_float)
        time_list = list()
        for i in range(len(qc_list_float)):
            time_list.append(i / 24)
        time_array = np.array(time_list)
        bc_data = np.column_stack((time_array, bc_data))

        # 记录原始属性
        tfa_data = bc_dataset.attrs['2D Flow Area']
        bl_data = bc_dataset.attrs['BC Line']
        cts_data = bc_dataset.attrs['Check TW Stage']
        dt_data = bc_dataset.attrs['Data Type']
        esfdf_data = bc_dataset.attrs['EG Slope For Distributing Flow']
        ed_data = bc_dataset.attrs['End Date']
        ff_data = bc_dataset.attrs['Face Fraction']
        fi_data = bc_dataset.attrs['Face Indexes']
        fpi_data = bc_dataset.attrs['Face Point Indexes']
        i_data = bc_dataset.attrs['Interval']
        ni_data = bc_dataset.attrs['Node Index']
        sd_data = bc_dataset.attrs['Start Date']

        # 删除旧的 dataset
        del f['Event Conditions']['Unsteady']['Boundary Conditions']['Flow Hydrographs'][dataset_name]

        # 创建新的 dataset
        new_bc_dataset = f['Event Conditions']['Unsteady']['Boundary Conditions']['Flow Hydrographs'].create_dataset(
            dataset_name, data=bc_data)

        # 恢复原来的属性，并修改开始和结束时间
        # 在新的Dataset中按照记录下来的属性重新新建
        new_bc_dataset.attrs.create('2D Flow Area', tfa_data, dtype=tfa_data.dtype)
        new_bc_dataset.attrs.create('BC Line', bl_data, dtype=bl_data.dtype)
        new_bc_dataset.attrs.create('Check TW Stage', cts_data, dtype=cts_data.dtype)
        new_bc_dataset.attrs.create('Data Type', dt_data, dtype=dt_data.dtype)
        new_bc_dataset.attrs.create('EG Slope For Distributing Flow', esfdf_data, dtype=esfdf_data.dtype)
        new_bc_dataset.attrs.create('Face Fraction', ff_data, dtype=ff_data.dtype)
        new_bc_dataset.attrs.create('Face Indexes', fi_data, dtype=fi_data.dtype)
        new_bc_dataset.attrs.create('Face Point Indexes', fpi_data, dtype=fpi_data.dtype)
        new_bc_dataset.attrs.create('Interval', i_data, dtype=i_data.dtype)
        new_bc_dataset.attrs.create('Node Index', ni_data, dtype=ni_data.dtype)

        new_bc_dataset.attrs.create('End Date', end_date, dtype=ed_data.dtype)
        new_bc_dataset.attrs.create('Start Date', start_date, dtype=sd_data.dtype)

    def _modify_normal_depths(self, f, start_date, end_date, dataset_name):
        """
        修改Normal Depth边界条件的Start Date和End Date
        :param f: h5py 文件对象
        :param start_date: 模拟开始时间
        :param end_date: 模拟结束时间
        :param dataset_name: hdf5 中的 dataset 路径
        """
        nd_dataset = f['Event Conditions']['Unsteady']['Boundary Conditions']['Normal Depths'][dataset_name]

        # 记录原始属性
        nd_data = nd_dataset[:]
        tfa_data = nd_dataset.attrs['2D Flow Area']
        bl_data = nd_dataset.attrs['BC Line']
        bls_data = nd_dataset.attrs['BC Line WS']
        cts_data = nd_dataset.attrs['Check TW Stage']
        ed_data = nd_dataset.attrs['End Date']
        ff_data = nd_dataset.attrs['Face Fraction']
        fi_data = nd_dataset.attrs['Face Indexes']
        fpi_data = nd_dataset.attrs['Face Point Indexes']
        i_data = nd_dataset.attrs['Interval']
        ni_data = nd_dataset.attrs['Node Index']
        sd_data = nd_dataset.attrs['Start Date']

        # 删除旧的 dataset
        del f['Event Conditions']['Unsteady']['Boundary Conditions']['Normal Depths'][dataset_name]

        # 创建新的 dataset
        new_bc_dataset = f['Event Conditions']['Unsteady']['Boundary Conditions']['Normal Depths'].create_dataset(
            dataset_name, data=nd_data)

        # 恢复原来的属性，并修改开始和结束时间
        # 在新的Dataset中按照记录下来的属性重新新建
        new_bc_dataset.attrs.create('2D Flow Area', tfa_data, dtype=tfa_data.dtype)
        new_bc_dataset.attrs.create('BC Line', bl_data, dtype=bl_data.dtype)
        new_bc_dataset.attrs.create('BC Line WS', bls_data, dtype=bls_data.dtype)
        new_bc_dataset.attrs.create('Check TW Stage', cts_data, dtype=cts_data.dtype)
        new_bc_dataset.attrs.create('Face Fraction', ff_data, dtype=ff_data.dtype)
        new_bc_dataset.attrs.create('Face Indexes', fi_data, dtype=fi_data.dtype)
        new_bc_dataset.attrs.create('Face Point Indexes', fpi_data, dtype=fpi_data.dtype)
        new_bc_dataset.attrs.create('Interval', i_data, dtype=i_data.dtype)
        new_bc_dataset.attrs.create('Node Index', ni_data, dtype=ni_data.dtype)

        new_bc_dataset.attrs.create('End Date', end_date, dtype=ed_data.dtype)
        new_bc_dataset.attrs.create('Start Date', start_date, dtype=sd_data.dtype)

    def _modify_xianghongdian(self, f, start_date, end_date, mean=1000.0, std_dev=10.0):
        xhd_dataset = f['Event Conditions']['Unsteady']['Boundary Conditions']['Flow Hydrographs']['2D: Perimeter 1 BCLine: Xianghongdian Inflow']

        # 按正态分布随机生成给定时间范围内的逐时流量过程
        date_range = pd.date_range(self.ymdhm_start, self.ymdhm_end, freq='H')
        flow_data = np.random.normal(loc=mean, scale=std_dev, size=len(date_range))
        # 生成符合hdf要求的时间序列格式
        time_list = list()
        for i in range(len(flow_data)):
            time_list.append(i / 24)
        time_array = np.array(time_list)
        # 返回符合hdf要求的时间-流量过程
        flow_data = np.column_stack((time_array, flow_data))

        # 记录原始属性
        tfa_data = xhd_dataset.attrs['2D Flow Area']
        bl_data = xhd_dataset.attrs['BC Line']
        cts_data = xhd_dataset.attrs['Check TW Stage']
        dt_data = xhd_dataset.attrs['Data Type']
        esfdf_data = xhd_dataset.attrs['EG Slope For Distributing Flow']
        ed_data = xhd_dataset.attrs['End Date']
        ff_data = xhd_dataset.attrs['Face Fraction']
        fi_data = xhd_dataset.attrs['Face Indexes']
        fpi_data = xhd_dataset.attrs['Face Point Indexes']
        i_data = xhd_dataset.attrs['Interval']
        ni_data = xhd_dataset.attrs['Node Index']
        sd_data = xhd_dataset.attrs['Start Date']

        # 删除旧的 dataset
        del f['Event Conditions']['Unsteady']['Boundary Conditions']['Flow Hydrographs']['2D: Perimeter 1 BCLine: Xianghongdian Inflow']

        # 创建新的 dataset
        new_bc_dataset = f['Event Conditions']['Unsteady']['Boundary Conditions']['Flow Hydrographs'].create_dataset(
            '2D: Perimeter 1 BCLine: Xianghongdian Inflow', data=flow_data)

        # 恢复原来的属性，并修改开始和结束时间
        # 在新的Dataset中按照记录下来的属性重新新建
        new_bc_dataset.attrs.create('2D Flow Area', tfa_data, dtype=tfa_data.dtype)
        new_bc_dataset.attrs.create('BC Line', bl_data, dtype=bl_data.dtype)
        new_bc_dataset.attrs.create('Check TW Stage', cts_data, dtype=cts_data.dtype)
        new_bc_dataset.attrs.create('Data Type', dt_data, dtype=dt_data.dtype)
        new_bc_dataset.attrs.create('EG Slope For Distributing Flow', esfdf_data, dtype=esfdf_data.dtype)
        new_bc_dataset.attrs.create('Face Fraction', ff_data, dtype=ff_data.dtype)
        new_bc_dataset.attrs.create('Face Indexes', fi_data, dtype=fi_data.dtype)
        new_bc_dataset.attrs.create('Face Point Indexes', fpi_data, dtype=fpi_data.dtype)
        new_bc_dataset.attrs.create('Interval', i_data, dtype=i_data.dtype)
        new_bc_dataset.attrs.create('Node Index', ni_data, dtype=ni_data.dtype)

        new_bc_dataset.attrs.create('End Date', end_date, dtype=ed_data.dtype)
        new_bc_dataset.attrs.create('Start Date', start_date, dtype=sd_data.dtype)

    def read_dataset(self, dataset_name: str):
        """
        从HDF文件中读取dataset中的数据
        :param dataset_name: dataset的名称
        :return: 返回读取到的数据集，类型为np.ndarray
        """
        f = h5py.File(self.filepath, 'r')
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



# 下面是用于临时测试的代码
if __name__ == '__main__':
    hdf_handler = HDFHandler(r'D:\Desktop\Foziling_Model_1030\FZLallbackup.p01.hdf')
    # hdf_handler.remove_hdf_results()
    qc_list = [2.57, 2.51, 1.97, 1.97, 1.96, 1.95, 1.94, 1.93, 1.92, 1.91, 1.9, 1.89, 1.88, 1.87, 1.86, 1.85, 1.84,
               1.83, 1.82,
               1.81, 1.8, 1.79, 1.78, 1.77, 1.76, 1.75, 1.74, 1.73, 1.72, 1.71, 1.7, 1.69, 1.68, 1.67, 1.66, 1.65, 1.64,
               1.63,
               1.62, 1.61, 1.6, 1.59, 1.58, 1.57, 1.56, 1.55, 1.54, 1.53, 1.52, 1.51, 1.5, 1.49, 1.48, 1.47, 1.46, 1.45,
               1.44,
               1.43, 1.42, 1.41, 1.4, 1.39, 1.38, 1.37, 1.36, 1.35, 1.34, 1.33, 1.32, 1.31, 1.3, 1.29, 1.28, 1.27, 1.26,
               1.25,
               1.24, 1.23, 1.23, 1.22, 1.21, 1.2, 1.2, 1.19, 1.19, 1.18, 1.17, 1.17, 1.16, 1.16, 1.15, 1.15, 1.14, 1.13,
               1.13,
               1.12, 1.12, 1.11, 1.11, 1.1, 1.09, 1.09, 1.08, 1.08, 1.07, 1.07, 1.06, 1.06, 1.05, 1.05, 1.04, 1.04,
               1.03, 1.03,
               1.02, 1.02, 1.01, 1.01, 1.0, 1.0, 0.99, 0.99, 0.98, 0.98, 0.97, 0.97, 0.96, 0.96, 0.95, 0.95, 0.94, 0.94,
               0.93,
               0.93, 0.92, 0.92, 0.91, 0.91, 0.91, 0.9, 0.9, 0.89, 0.89, 0.88]
    # hdf_handler.modify_boundary_conditions(qc_list, '26Mar2023 0900', '01Apr2023 0800')
    f = h5py.File(r'D:\Desktop\Foziling_Model_1030\FZLallbackup.p01.hdf', 'a')
    # hdf_handler._modify_normal_depths(f, '26Mar2023 0900', '01Apr2023 0800', '2D: Perimeter 1 BCLine: Hengpaitou Outflow')

    hdf_handler.modify_plan_data('26Mar2023 09:00:00', '01Apr2023 08:00:00')
