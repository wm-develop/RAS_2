模型部署方法：

1. 将`raspackage_linux.zip`解压到本地anaconda3的环境目录（该文件另附）
2. 将`Foziling_Model_1030.zip`解压到本地（该文件另附）
3. 修改`RAS_2/config.py`中的相关参数，包括sql server的相关信息以及上一步模型目录的路径
4. 该模型在Ubuntu 22.04系统下测试通过，如果部署该模型的操作系统为Centos，需要将`Foziling_Model_1030\run_unsteady.sh`中的`RAS_LIB_PATH`设置为`RAS_LIB_PATH=./libs:./libs/mkl:./libs/centos_7 `
5. 给予`Foziling_Model_1030`目录中的`run_unsteady.sh`和`Ras_v61`目录中的所有文件执行权限
6. 在raspackage虚拟环境下，cd到RAS_2目录，执行：`python api_server.py`
7. 其他客户端发送post请求，将方案名传递过来。json的格式要求见`RAS_2/test.json`
8. 等待模型计算完成
9. `{scheme_name}_output.csv`（每个网格的水深）和`{scheme_name}_max_water_area.shp`（最大淹没面积shp图）保存到RAS_2目录下
