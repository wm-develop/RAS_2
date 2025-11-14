# HDF5输出功能测试脚本使用说明

## 脚本概述

`test_hdf5_output.py` 是一个独立的测试脚本，用于测试HDF5输出功能。该脚本跳过了模型计算前的所有逻辑（包括数据库连接、边界条件修改、模型运行等），直接从HEC-RAS模拟结果文件读取数据并生成HDF5输出。

## 使用步骤

### 1. 配置文件路径

打开 `test_hdf5_output.py`，修改以下配置参数（在脚本开头的配置部分）：

```python
# HEC-RAS结果文件路径
p01_hdf_path = r"D:\Desktop\fzl_history\Fzlmodel20251030\FZLall.p01.hdf"

# 输出路径
output_path = r"D:\Desktop\fzl_history\test_output"

# Shapefile路径（用于计算淹没面积）
shp_path = r"D:\Desktop\fzl_history\Fzlmodel20251030\fanwei\fanwei.shp"
```

**说明：**
- `p01_hdf_path`: HEC-RAS模拟结果文件的完整路径
- `output_path`: 测试输出文件的保存目录（脚本会自动创建）
- `shp_path`: 用于计算淹没面积的shapefile路径（必须包含Area字段）

### 2. 运行脚本

在命令行中运行：

```bash
python test_hdf5_output.py
```

或在IDE中直接运行该文件。

### 3. 检查输出

脚本成功运行后，会在指定的 `output_path` 目录下生成以下文件：

#### 主要输出文件
- **hydroModel.hdf5** - HDF5格式的模拟结果文件（主要输出）

#### 辅助输出文件
- **output.csv** - CSV格式的水深数据
- **bailianya.csv** - 白莲崖坝下水位数据
- **mozitan.csv** - 磨子潭坝下水位数据
- **foziling.csv** - 佛子岭坝下水位数据
- **max_water_area.shp** - 最大淹没面积shapefile（及相关文件）

## 脚本执行流程

1. ✅ 检查输入文件是否存在
2. ✅ 从HEC-RAS结果文件读取数据
   - Cells Minimum Elevation（网格最低高程）
   - Water Surface（水位）
3. ✅ 处理水深数据（水位 - 高程）
4. ✅ 保存CSV文件
5. ✅ 提取坝下水位数据
6. ✅ 计算最大淹没面积
7. ✅ 计算每个时刻的淹没面积
8. ✅ 创建HDF5输出文件

## HDF5文件内容

生成的 `hydroModel.hdf5` 文件包含以下数据集：

```
data/
├── 2DFlowAreas/
│   ├── WaterSurface - 二维流域的水位数据
│   ├── depth - 水深数据
│   └── FloodedArea - 每个时刻的淹没面积(km²)
├── CrossSections/
│   ├── WaterSurface - 断面水位
│   ├── Name - 断面名称
│   └── Flow - 断面流量
└── TimeDateStamp - 时间戳（已转换为标准格式）
```

## 验证HDF5文件

您可以使用以下Python代码验证生成的HDF5文件：

```python
import h5py

# 打开HDF5文件
with h5py.File('hydroModel.hdf5', 'r') as f:
    # 查看文件结构
    def print_structure(name, obj):
        print(name)
    
    f.visititems(print_structure)
    
    # 读取特定数据集
    depth = f['data']['2DFlowAreas']['depth'][:]
    flooded_area = f['data']['2DFlowAreas']['FloodedArea'][:]
    time_stamp = f['data']['TimeDateStamp'][:]
    
    print(f"\n水深数据形状: {depth.shape}")
    print(f"淹没面积形状: {flooded_area.shape}")
    print(f"时间戳数量: {len(time_stamp)}")
    print(f"\n前5个时间戳:")
    for i in range(min(5, len(time_stamp))):
        print(f"  {time_stamp[i].decode('utf-8')}")
```

## 常见问题

### Q1: 提示"HEC-RAS结果文件不存在"
**A:** 请检查 `p01_hdf_path` 路径是否正确，确保文件存在且路径使用原始字符串（r""）

### Q2: 提示"Shapefile不存在"
**A:** 如果shapefile路径不正确，脚本会跳过淹没面积计算，但仍会生成HDF5文件（FloodedArea数据集为None）

### Q3: 如何查看详细的执行日志？
**A:** 日志会输出到控制台，同时也会保存到 `logs/ras.log` 文件中

### Q4: 生成的HDF5文件可以用什么工具查看？
**A:** 
- Python: h5py库
- HDFView: HDF Group提供的图形界面工具
- Python工具: h5dump命令行工具

## 注意事项

1. 确保HEC-RAS结果文件包含完整的模拟结果
2. Shapefile必须包含 `Area` 字段（网格面积，单位：m²）
3. 如果要修改淹没面积的基准值（当前为42756184m²），请在脚本中搜索并修改该值
4. 测试脚本不会修改任何输入文件，只会在输出目录中创建新文件

## 技术支持

如遇到问题，请检查：
1. 日志文件中的详细错误信息
2. 输入文件的格式是否正确
3. Python环境中是否安装了所需的依赖包（h5py, geopandas, numpy等）
