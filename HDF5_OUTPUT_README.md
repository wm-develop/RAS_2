# HDF5输出功能说明

## 概述
本次修改为HEC-RAS模拟程序添加了新的HDF5格式输出功能，用于替代原有的CSV输出。

## 新增文件

### output_hdf_handler.py
包含两个主要函数：

1. **convert_time_date_stamp(time_date_stamp_array)**
   - 功能：将HEC-RAS的时间格式转换为标准格式
   - 输入格式：'09APR2025 00:10:00'
   - 输出格式：'YYYY-MM-DD HH:MM:SS'

2. **create_output_hdf5(output_path, hdf_handler, depth_data, flooded_area, logger)**
   - 功能：创建符合要求的HDF5输出文件
   - 输出文件：hydroModel.hdf5

## HDF5文件结构

```
data (Group)
├── 2DFlowAreas (Group)
│   ├── WaterSurface (dataset)  # 从hdf_handler.read_dataset('Water Surface')获取
│   ├── depth (dataset)          # 从depth_data变量获取
│   └── FloodedArea (dataset)    # 每个时刻的淹没面积(km²)
├── CrossSections (Group)
│   ├── WaterSurface (dataset)  # 从HDF5结果文件读取
│   ├── Name (dataset)          # 从HDF5结果文件读取
│   └── Flow (dataset)          # 从HDF5结果文件读取
└── TimeDateStamp (dataset)     # 从HDF5结果文件读取并转换格式
```

## 数据来源

### 直接从代码获取
- **2DFlowAreas/WaterSurface**: `hdf_handler.read_dataset('Water Surface')`
- **2DFlowAreas/depth**: `depth_data` (主程序变量)
- **2DFlowAreas/FloodedArea**: 计算得到的每个时刻的淹没面积

### 淹没面积计算方法
FloodedArea数据集存储每个时刻的淹没面积，单位为km²，计算方法：
1. 对每个时间步，遍历所有网格
2. 如果网格水深 > 0.2m，则将该网格的面积累加
3. 总面积减去42756184m²（基准面积）
4. 转换为km²（除以1000000）
5. 如果结果 < 0，则设为0

### 从HEC-RAS输出的HDF5文件读取
- **CrossSections/WaterSurface**: 
  ```python
  f['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['Reference Lines']['Water Surface'][:]
  ```
  
- **CrossSections/Name**: 
  ```python
  f['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['Reference Lines']['Name'][:]
  ```
  
- **CrossSections/Flow**: 
  ```python
  f['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['Reference Lines']['Flow'][:]
  ```
  
- **TimeDateStamp**: 
  ```python
  f['Results']['Unsteady']['Output']['Output Blocks']['Base Output']['Unsteady Time Series']['Time Date Stamp'][:]
  ```
  注意：此数据会经过格式转换

## 主程序修改 (api_server_docker.py)

在原有CSV输出位置添加了HDF5输出功能：

```python
# 创建新的HDF5输出文件
hdf5_success = create_output_hdf5(output_path, hdf_handler, depth_data, logger)
if not hdf5_success:
    logger.warning("HDF5输出文件创建失败，但程序继续运行")

# 保留原CSV输出（暂时不变）
csv_path = output_path + os.path.sep + "output.csv"
insert_time_and_save_to_csv(depth_data, csv_path)
logger.info(f"水深数据已写入到{csv_path}")
```

## 特点

1. **最小化修改**: 只在必要位置添加代码，不影响原有逻辑
2. **保留CSV输出**: 原有CSV输出功能保持不变
3. **容错处理**: HDF5输出失败不会影响程序继续运行
4. **完整日志**: 详细记录每个数据读取和写入步骤
5. **格式转换**: 自动将时间戳转换为标准格式

## 使用说明

程序运行后，在 `/root/results/` 目录下会生成：
- `hydroModel.hdf5` - 新的HDF5格式输出文件
- `output.csv` - 原有的CSV格式文件（保留）

## 注意事项

1. 如果HEC-RAS结果文件中缺少某些数据集，相应的输出数据集也会缺失，但程序会继续运行
2. 时间格式转换失败时会保留原始时间格式并记录警告
3. HDF5文件创建失败时会记录警告，但不会中断整个流程
