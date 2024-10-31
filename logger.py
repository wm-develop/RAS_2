#!/usr/bin/python3
# -*- encoding: utf-8 -*-
"""
@File    :   logger.py
@Desc    :   日志模块
@Version :   v1.0
@Time    :   2023/05/16
@Author  :   xiaoQQya
@Contact :   xiaoQQya@126.com
"""
import os
import logging
from logging import handlers, Logger


def set_logger(log_name: str, log_path: str) -> Logger:
    """配置日志

    :param log_name: 日志记录器名称
    :type log_name: str
    :param log_path: 日志保存路径
    :type log_path: str
    :return: 日志记录器
    :rtype: Logger
    """
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(threadName)s] %(levelname)s %(module)s -> %(message)s",
        "%Y-%m-%d %H:%M:%S"
    )

    default_handler = logging.StreamHandler()
    default_handler.setFormatter(formatter)

    file_handler = handlers.TimedRotatingFileHandler(
        filename=log_path,
        when="midnight",
        interval=1,
        backupCount=15,
        encoding="utf-8",
        delay=False,
        utc=False
    )
    file_handler.suffix = "%Y-%m-%d.log"
    file_handler.setFormatter(formatter)

    logger.addHandler(default_handler)
    logger.addHandler(file_handler)
    return logger


if not os.path.exists("logs"):
    os.makedirs("logs")
logger: Logger = set_logger("ras", os.path.join("logs", "ras"))
