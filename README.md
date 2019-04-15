This project is to use zeromq with heartbeat enabled.

With current usage of zmq, updaterd occurs bug behavior:

tcp connection still exists in both bitmarkd and udpaterd, but updaterd zeromq client receives no message.
