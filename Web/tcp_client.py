from threading import Thread, current_thread
import socket


def x_client():
    client = socket.socket()
    client.connect(('127.0.0.1', 8080))
    n = 0
    while True:
        msg = '%s say hello %s' % (current_thread().name, n)
        n += 1
        client.send(msg.encode('utf-8'))
        data = client.recv(1024)
        print(data.decode('utf-8'))


if __name__ == '__main__':
    for i in range(100):
        t = Thread(target=x_client)
        t.start()
