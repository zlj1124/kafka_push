from confluent_kafka import Consumer
from logger.logd import log


mess_conf = Consumer({
    'bootstrap.servers': 'alikafka-pre-cn-9lb3ejv7r001-2-vpc.alikafka.aliyuncs.com:9092',  # Kafka 服务器地址
    'group.id': 'CID_alikafka_svm_stroke_segmentation_ady_group',  # 消费者组 ID
    'auto.offset.reset': 'earliest'  # 从最早的消息开始消费
})
mess_conf.subscribe(['alikafka_svm_topic_can_data_topic'])


def consume_kafka():
    try:
        while True:
            msg = mess_conf.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.info(f"Consumer error: {msg.error()}")
                continue
            log.info(f'Received message: {msg.value()}')
    except KeyboardInterrupt:
        log.info('Stopping consumer')
    finally:
        mess_conf.close()


if __name__ == "__main__":
    consume_kafka()
