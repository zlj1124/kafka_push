# -*- coding=UTF-8 -*-
from logger.logd import log
from confluent_kafka import Producer
import time
import random
import ujson
import asyncio
import aiofiles
import signal


class KafkaDataProducer:
    def __init__(self):
        # 192.168.3.81:9092
        # alikafka-pre-cn-9lb3ejv7r001-1-vpc.alikafka.aliyuncs.com:9092
        kafka_bs = "alikafka-pre-cn-9lb3ejv7r001-1-vpc.alikafka.aliyuncs.com:9092, alikafka-pre-cn-9lb3ejv7r001-2-vpc.alikafka.aliyuncs.com:9092,alikafka-pre-cn-9lb3ejv7r001-3-vpc.alikafka.aliyuncs.com:9092"
        conf = {
            "bootstrap.servers": kafka_bs,
            'queue.buffering.max.messages': 10000000,  # 缓冲区最多10000000条消息
            'queue.buffering.max.kbytes': 2097152,    # 缓冲区最大2G
            'queue.buffering.max.ms': 1000,           # 最多等待1秒钟发送
            'batch.num.messages': 10000,              # 每批次最多包含10000条消息
            'linger.ms': 500,                         # 等待500ms以更多消息加入批次
            'compression.codec': 'gzip'              # 使用gzip压缩
        }
        self.producer = Producer(**conf)
        self.topic = "alikafka_svm_topic_can_data_topic"  # DZL_TEST
        self.gpslngat = [
            [116.478935, 39.997761],
            [116.478939, 39.997825],
            [116.478912, 39.998549],
            [116.478912, 39.998549],
            [116.478998, 39.998555],
            [116.478998, 39.998555],
            [116.479282, 39.998560],
            [116.479658, 39.998528],
            [116.480151, 39.998453],
            [116.480784, 39.998302],
            [116.480784, 39.998302],
            [116.481149, 39.998184],
            [116.481573, 39.997997],
            [116.481863, 39.997846],
            [116.482072, 39.997718],
            [116.482362, 39.997718],
            [116.483633, 39.998935],
            [116.483670, 39.998968],
            [116.484648, 39.999861],
            [116.484648, 39.999861],
            [116.483670, 39.998968],
            [116.483633, 39.998935],
            [116.482362, 39.997718],
            [116.482072, 39.997718],
            [116.481863, 39.997846],
            [116.481573, 39.997997],
            [116.481149, 39.998184],
            [116.480784, 39.998302],
            [116.480784, 39.998302],
            [116.480151, 39.998453],
            [116.479658, 39.998528],
            [116.479282, 39.998560],
            [116.478998, 39.998555],
            [116.478998, 39.998555],
            [116.478912, 39.998549],
            [116.478912, 39.998549],
            [116.478939, 39.997825],
            [116.478935, 39.997761],]

    def timestamptime(self):
        return int(time.time())

    async def read_data(self):
        vin_list = []
        try:
            async with aiofiles.open("./datafile/YC.txt", "r") as f:
                async for line in f:
                    vin_list.append(line.strip())
        except FileNotFoundError:
            log.info("文件不存在,请检查文件路径和名称!")
        except IOError as e:
            log.info(f"读取文件发生错误: {e}")
        return vin_list

    async def put_kafka(self, vin):
        try:
            mileage = 90853.95
            while True:
                for coodres in self.gpslngat:
                    timer = self.timestamptime()
                    speed = round(random.randint(0, 220) / 10, 2)
                    # speed = 0
                    if speed == 0:
                        lng, lat = 116.478939, 39.997825
                    else:
                        lng, lat = coodres[0], coodres[1]
                    mileage += speed
                    mess = ujson.dumps({
                        "vin": vin,
                        "vehicle_status": 1,
                        "timer": timer,
                        "position_status": 0,
                        "lng_flag": 0,
                        "lat_flag": 0,
                        "lng": lng,  # 116.478939
                        "lat": lat,  # 39.997825
                        "speed": speed,
                        "mileage": round(mileage, 2)
                    })
                    self.producer.produce(self.topic, mess)
                    self.producer.poll(0)
                    log.info(f"{mess}")
                    # timer += 10
                    await asyncio.sleep(5)
        except BufferError:
            log.info(f"本地生产队列已满请等待重试...")
            await asyncio.sleep(1)
        except Exception as e:
            log.error(f"{e}")

    async def pl_push(self):
        """异步(批量vin)"""
        vins = await self.read_data()
        await asyncio.gather(
            *[self.put_kafka(vin) for vin in vins])

    async def dg_push(self):
        """指定vin"""
        vins = ["FPQPKFXAANO588163"]
        await asyncio.gather(
            *[self.put_kafka(vin) for vin in vins])

    async def main(self):
        await self.dg_push()
        # await self.pl_push()

    def flush(self):
        self.producer.flush()

    def handle_sigint(self, signal, frame):
        log.info(f"收到信号flushing队列消息...")
        self.flush()
        log.info(f"flush队列消息完成退出...")
        exit(0)

    def run(self):
        signal.signal(signal.SIGINT, self.handle_sigint)
        asyncio.run(self.main())
        self.flush()


if __name__ == '__main__':
    push = KafkaDataProducer()
    # asyncio.run(push.main())
    push.run()
