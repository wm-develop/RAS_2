模型部署方法：

1. 将raspackage_linux.zip解压到本地anaconda3的环境目录
2. 将Foziling_Model_1030.zip解压到本地
3. 修改RAS_2/config.py中的相关参数，包括sql server的相关信息以及上一步模型目录的路径
4. 在raspackage环境下，cd到RAS_2目录，执行：`python api_server.py`
5. 其他客户端发送post请求，将方案名传递过来。json的格式要求见RAS_2/test.json
6. 等待模型计算完成
7. `{scheme_name}_output.csv`（每个网格的水深）和`{scheme_name}_max_water_area.shp`（最大淹没面积shp图）保存到RAS_2目录下
