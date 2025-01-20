# -*- coding:UTF-8 -*-
# author: duzhonglin
# date: 2024/11/11
# Description: 日志配置

import logging
import logging.config
# from logging.handlers import RotatingFileHandler
import logging.handlers

logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'  # 日期时间格式
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',  # 输出到控制台
            'formatter': 'standard'
        },
        'rotating_file': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',  # 输出到文件
            'formatter': 'standard',         # 格式化器名称
            'filename': './log/app.log',     # 日志文件名称
            'mode': 'a',                     # 写入模式，'a' 为追加模式
            'maxBytes': 50 * 1024 * 1024,     # 文件大小上限为 50 MB
            'backupCount': 5                 # 最多保留 5 个备份文件
        },
    },
    'loggers': {
        '': {
            'handlers': ['console', 'rotating_file'],
            'level': 'INFO',
            'propagate': False  # Fasle 则不向上级日志记录
        },
    }
}

logging.config.dictConfig(logging_config)
log = logging.getLogger()
