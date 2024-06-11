# Copy file lên container
docker cp ./data/danh_sach_sv_de.csv namenode:hadoop-data
# Truy cập vào cửa sổ dòng lệnh của Namenode container
docker exec -it namenode bash
# Tạo các directory
hdfs dfs -mkdir -p user/root/data
hdfs dfs -mkdir -p user/root/raw_zone/fact/activity
hdfs dfs -mkdir user/root/result
# Đẩy file từ container lên HDFS
hdfs dfs -put hadoop-data/danh_sach_sv_de.csv user/root/data
