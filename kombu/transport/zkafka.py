"""
kombu.transport.kafka
=====================

Kafka transport.

:copyright: (c) 2010 - 2013 by Mahendra M.
:license: BSD, see LICENSE for more details.

**Synopsis**

Takes a Zookeeper instance as broker, discovers the kafka brokers for
the topic and sends/consumes data
Connects to Zookeeper as <server>:<port>/<vhost>
The <vhost> becomes the group for all the clients. So we can use
it like a vhost

It is recommended that the queue be created in advance, by specifying the
number of partitions. The partition configuration determines how many
consumers can fetch data in parallel
"""

from __future__ import absolute_import

import socket

from anyjson import loads, dumps

from kombu.exceptions import StdConnectionError, StdChannelError
from kombu.five import Empty

from . import virtual

try:
    import kafka
    import kazoo
    from kazoo.client import KazooClient
    from kafka.zookeeper import ZSimpleConsumer, ZSimpleProducer, get_client

    ZKAFKA_CONNECTION_ERRORS = ()
    ZKAFKA_CHANNEL_ERRORS = ()

except ImportError:
    kafka = None                                            # noqa
    kazoo = None
    ZKAFKA_CONNECTION_ERRORS = ZKAFKA_CHANNEL_ERRORS = ()   # noqa

DEFAULT_PORT = 2181

__author__ = 'Mahendra M <mahendra.m@gmail.com>'


class Channel(virtual.Channel):

    _client = None
    _zkafka_consumers = {}
    _zkafka_producers = {}

    def _get_producer(self, queue):
        """Create/get a producer instance for the given topic/queue"""

        producer = self._zkafka_producers.get(queue, None)
        if producer is None:
            producer = ZSimpleProducer(self.host, queue)
            self._zkafka_producers[queue] = producer

        return producer

    def _get_consumer(self, queue):
        """
        Create/get a consumer instance for the given topic/queue
        """
        consumer = self._zkafka_consumers.get(queue, None)
        if consumer is None:
            consumer = ZSimpleConsumer(self.host, self.group, queue,
                                       auto_commit=True,
                                       auto_commit_every_n = 20,
                                       auto_commit_every_t = 5000)
            self._zkafka_consumers[queue] = consumer

        return consumer

    def _put(self, queue, message, **kwargs):
        """Put a message on the topic/queue"""
        producer = self._get_producer(queue)
        producer.send_messages(dumps(message))

    def _get(self, queue):
        """Get a message from the topic/queue"""
        consumer = self._get_consumer(queue)

        msgs = consumer.get_messages(count=1)
        if not msgs:
            raise Empty()

        return loads(msgs[0].message.value)

    def _purge(self, queue):
        """Purge all pending messages in the topic/queue"""
        consumer = self._get_consumer(queue)

        # Seek to the end of the queues and commit
        consumer.seek(0,2)
        consumer.commit()

    def _delete(self, queue, *args, **kwargs):
        """Delete a queue/topic"""
        # We will just let it go through. There is no API defined yet
        # for deleting a queue/topic
        pass

    def _size(self, queue):
        """Gets the number of pending messages in the topic/queue"""
        consumer = self._get_consumer(queue)
        return consumer.pending()

    def _new_queue(self, queue, **kwargs):
        """Create a new queue if it does not exist"""
        # Just create a producer, the queue will be created automatically
        self._get_producer(queue)

    def _has_queue(self, queue):
        """Check if a queue already exists"""

        # TODO: This must be made as a public API in the kafka library
        self._client._load_metadata_for_topics()
        exists = queue in self._client.topic_partitions

        return exists

    def _open(self):
        conninfo = self.connection.client
        port = conninfo.port or DEFAULT_PORT
        host = conninfo.hostname

        self.host = '%s:%s' % (self.host, self.port)
        self.group = conninfo.virtual_host[0:-1]

        zkclient = KazooClient(self.host)
        zkclient.start()

        client = get_client(zkclient)
        zkclient.stop()
        zkclient.close()
        return client

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client


class Transport(virtual.Transport):
    Channel = Channel
    polling_interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (StdConnectionError, ) + ZKAFKA_CONNECTION_ERRORS
    channel_errors = (StdChannelError, socket.error) + ZKAFKA_CHANNEL_ERRORS
    driver_type = 'zkafka'
    driver_name = 'zkafka'

    def __init__(self, *args, **kwargs):
        if kafka is None:
            raise ImportError('The kafka library is not installed')

        super(Transport, self).__init__(*args, **kwargs)

    def driver_version(self):
        return kafka.__version__
