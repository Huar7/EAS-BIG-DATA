import kafka as kf
import pyspark

consumer = kf.KafkaConsumer('main',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         group_id='my_group_id',
                         value_deserializer=lambda x: x.decode('utf-8')
                        )
consumer.subscribe(topics=['main'])

# Poll for new messages
while True:
    msg = consumer.poll(timeout_ms=1000)
    if msg:
        for key, value in msg.items():
            print("Key: {} | Value: {}".format(key, value))
    else:
        print("No new messages")
