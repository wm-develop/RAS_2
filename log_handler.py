import logging


class ImmediateFileHandler(logging.FileHandler):
    def emit(self, record):
        # 调用父类的emit方法写入日志
        super().emit(record)
        # 刷新流以确保日志立即写入文件
        self.stream.flush()
