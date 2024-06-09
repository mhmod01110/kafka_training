from confluent_kafka import Producer
import random
import random
import string
characters = string.ascii_letters + string.digits



me="Mahmoud_NEW"
conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'client.id': me}
for i in range(100):
        new_msg="new_message"+str(i)
        random_string = ''.join(random.choice(characters) for _ in range(4))
        producer = Producer(conf)
        producer.produce(me, key=random_string, value=new_msg)
        producer.flush()