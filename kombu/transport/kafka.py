"""
kombu.transport.kafka
=====================

Kafka transport.

:copyright: (c) 2010 - 2013 by Mahendra M.
:license: BSD, see LICENSE for more details.

**Synopsis**

Connects to kafka (0.8.x) as <server>:<port>/<vhost>
The <vhost> becomes the group for all the clients. So we can use
it like a vhost

It is recommended that the queue be created in advance, by specifying the
number of partitions. The partition configuration determines how many
consumers can fetch data in parallel

**Limitations**
* The client API needs to modified to fetch data only from a single
  partition. This can be used for effective load balancing also.

**Known issues**

* The offsets are not committed. There is a bug in the upstream kafka
  library which needs to be fixed -
  https://github.com/mumrah/kafka-python/issues/18

* On restart, the client will replay messages from the beginning. This is
  because of a bug in offset commit fetch request -
  https://github.com/mumrah/kafka-python/issues/21

"""

from __future__ import absolute_import

import socket

from anyjson import loads, dumps

from kombu.exceptions import StdConnectionError, StdChannelError
from kombu.five import Empty

from . import virtual

try:
    import kafka
    from kafka.client import KafkaClient
    from kafka.consumer import SimpleConsumer
    from kafka.producer import SimpleProducer

    KAFKA_CONNECTION_ERRORS = ()
    KAFKA_CHANNEL_ERRORS = ()

except ImportError:
    kafka = None                                          # noqa
    KAFKA_CONNECTION_ERRORS = KAFKA_CHANNEL_ERRORS = ()   # noqa

DEFAULT_PORT = 9092

__author__ = 'Mahendra M <mahendra.m@gmail.com>'


class Channel(virtual.Channel):

    _client = None
    _kafka_group = None
    _kafka_consumers = {}
    _kafka_producers = {}

    def _get_producer(self, queue):
        """Create/get a producer instance for the given topic/queue"""

        producer = self._kafka_producers.get(queue, None)
        if producer is None:
            producer = SimpleProducer(self.client, queue)
            self._kafka_producers[queue] = producer

        return producer

    def _get_consumer(self, queue):
        """
        Create/get a consumer instance for the given topic/queue
        """
        consumer = self._kafka_consumers.get(queue, None)
        if consumer is None:
            # TODO: Enable auto_commit once upstream bugs are fixed
            consumer = SimpleConsumer(self.client, self._kafka_group, queue,
                                      auto_commit=False)
            self._kafka_consumers[queue] = consumer

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
        for msg in consumer:
            pass

        # TODO: Force a commit once messages have been read
        # consumer.commit()

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

        client = self._open()

        # TODO: This must be made as a public API in the kafka library
        client._load_metadata_for_topics()
        exists = queue in client.topic_partitions
        client.close()

        return exists

    def _open(self):
        conninfo = self.connection.client
        port = conninfo.port or DEFAULT_PORT
        client = KafkaClient(conninfo.hostname, int(port))
        return client

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
            self._kafka_group = self.connection.client.virtual_host
        return self._client


class Transport(virtual.Transport):
    Channel = Channel
    polling_interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (StdConnectionError, ) + KAFKA_CONNECTION_ERRORS
    channel_errors = (StdChannelError, socket.error) + KAFKA_CHANNEL_ERRORS
    driver_type = 'kafka'
    driver_name = 'kafka'

    def __init__(self, *args, **kwargs):
        if kafka is None:
            raise ImportError('The kafka library is not installed')

        super(Transport, self).__init__(*args, **kwargs)

    def driver_version(self):
        return kafka.__version__
