from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import requests
import random
import json
import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont


# YOLO model files
YOLO_CONFIG = 'yolov3.cfg'
YOLO_WEIGHTS = 'yolov3.weights'
YOLO_CLASSES = 'coco.names'


# Load YOLO model
net = cv2.dnn.readNet(YOLO_WEIGHTS, YOLO_CONFIG)
with open(YOLO_CLASSES, 'r') as f:
    classes = [line.strip() for line in f.readlines()]

layer_names = net.getLayerNames()
output_layers = [layer_names[i - 1] for i in net.getUnconnectedOutLayers()]




me="Mahmoud_NEW_2"
conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'group.id': me,
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'smallest'}

Consumer = Consumer(conf)
running = True


# def detect_object(id):
#     return random.choice(['car', 'house', 'person'])

def add_watermark(image_path, text):
    # Load image
    img_pil = Image.open(image_path)
    draw = ImageDraw.Draw(img_pil)
    width, height = img_pil.size

    # Use a larger font size for the watermark
    font_size = int(min(width, height) / 6)
    font = ImageFont.truetype("arial.ttf", font_size)  # Ensure you have Arial font or change to an available font
    text_size = draw.textsize(text, font)
    text_position = ((width - text_size[0]) / 2, (height - text_size[1]) / 2)
    draw.text(text_position, text, font=font, fill=(255, 255, 255, 128))

    # Save watermarked image
    img_pil.save(image_path)


def msg_process(msg):
    try:
        msg_value = msg.value()
        if msg_value is None:
            print("Received an empty message.")
            return

        print(f"Received message: {msg_value}")

        msg_dict = json.loads(msg_value)
        id = msg_dict["id"]
        filename = msg_dict["filename"]
        watermark_text = msg_dict.get("watermark", "Sample Watermark")

        # Add watermark to the image
        add_watermark(f"images/{filename}", watermark_text)
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")



def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


basic_consume_loop(Consumer, [me])