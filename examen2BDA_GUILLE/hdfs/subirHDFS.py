import os
from pywebhdfs.webhdfs import PyWebHdfsClient

hdfs = PyWebHdfsClient(host='localhost', port='9870', user_name='root')

local_file_path = "../generadoresData/fake_data.csv"
hdfs_target_path = "/data/guille/fake_data.csv"

with open(local_file_path, 'r') as file:
    content = file.read()

hdfs.create_file(hdfs_target_path, content)
print(f"Archivo '{local_file_path}' subido a HDFS en '{hdfs_target_path}'")

# hdfs_result_path = "/user/root/nulos/part-m-00000"
# result_data = hdfs.read_file(hdfs_result_path)

# with open("result.csv", 'w') as result_file:
#     result_file.write(result_data)

# print("Resultado guardado en 'result.csv'")