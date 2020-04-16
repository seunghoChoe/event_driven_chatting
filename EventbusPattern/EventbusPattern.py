from queue import Queue
from threading import Thread
import threading
import time
import pickle

class Message(object):
    def __init__(self, user_name='', channel='', text=''):
        self.user_name = user_name
        self.channel = channel
        self.text = text

class Trigger(Thread):
    def __init__(self, name):
        Thread.__init__(self)
        self.name = name
        self.broker = None
        pass

    def run(self):
        while True:
            channel = input('channel: ')
            text = input('text: ')
            self.send_message(text, channel)
            if text == 'quit':
                break

    def register_broker(self, broker):
        self.broker = broker
        pass

    def send_message(self, text, channel):
        print(self.name, 'Send Event')
        message = Message(self.name, channel, text)
        self.broker.send(pickle.dumps(message))
        pass


class Broker(Thread):

    def __init__(self, name):
        Thread.__init__(self)
        self.name = name
        self.queue = Queue()
        self.worker_list = list()
        self.process_trigger = threading.Event()
        pass

    def send(self, message):
        self.queue.put(message)
        if not self.process_trigger.is_set():
            self.process_trigger.set()
            self.process_trigger.clear()

    def run(self):
        while True:
            if not self.queue.empty():
                print(self.name, 'Process Event')
                message = self.queue.get()
                self.process(message)

            else:
                print(self.name, 'Wait Event')
                self.process_trigger.wait()

    def register_worker(self, worker):
        print(self.name, 'Register', worker.name)
        self.worker_list.append(worker)

    def process(self, message):
        message = pickle.loads(message)
        for worker in self.worker_list:
            if worker.channel == message.channel:
                worker.process(pickle.dumps(message))


class Worker(object):
    def __init__(self, name, channel):
        self.name = name
        self.channel = channel
        pass

    def process(self, message):
        message = pickle.loads(message)
        print(self.name, 'process', message.user_name, message.channel, message.text)


if __name__ == '__main__':
    broker = Broker('Broker1')

    trigger = Trigger('Trigger1')
    trigger.register_broker(broker)
    trigger = Trigger('Trigger2')
    trigger.register_broker(broker)

    worker = Worker('Worker1', '1')
    broker.register_worker(worker)
    worker = Worker('Worker2', '2')
    broker.register_worker(worker)

    trigger.start()
    broker.start()
