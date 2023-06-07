import logging

# 创建 Logger 对象
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 创建 FileHandler 处理程序
file_handler = logging.FileHandler('app.log')
file_handler.setLevel(logging.INFO)

# 创建 Formatter 格式化器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 添加 FileHandler 处理程序
logger.addHandler(file_handler)