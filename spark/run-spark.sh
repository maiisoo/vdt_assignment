# Truy cập vào cửa sổ dòng lệnh của Spark container
docker exec -it spark-master bash
# Tạo thư mục lưu kết quả
mkdir result
# Submit job
spark-submit processing.py