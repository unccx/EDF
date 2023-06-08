import logging

log_level = logging.INFO

# 创建 Logger 对象
logger = logging.getLogger(__name__)
logger.setLevel(log_level) # logger记录的日志级别，大于等于这一级别的日志才会被记录

# 创建一个将日志信息输出到文件的 FileHandler 对象
file_handler = logging.FileHandler('app.log', mode="w")
file_handler.setLevel(log_level) # handler输出的日志级别，大于等于这一级别的日志才会输出到文件中

# 创建一个将日志信息输出到控制台的 StreamHandler 对象
console_handler = logging.StreamHandler()
console_handler.setLevel(log_level) # handler输出的日志级别

# 创建 Formatter 格式化器
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# 创建 Formatter 格式化器
console_formatter = logging.Formatter('%(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# 将 console_handler 和 file_handler 添加到 logger 对象中
logger.addHandler(console_handler)
logger.addHandler(file_handler)