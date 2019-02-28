import time
import cv2
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
#assigning_a_topic

topic = 'video'


def emitter(video):
    video = cv2.VideoCapture(video)
    print('sending......')
    while video.isOpened():
        success, image = video.read()
        if not success:
            break
        ret, jpeg = cv2.imencode('.png', image)
        producer.send(topic, jpeg.tobytes())
        time.sleep(0.1)

    video.release()
    print('done emitting')


if __name__ == '__main__':
    emitter('video.mp4')
