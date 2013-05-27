from nose import SkipTest

from funtests import transport


class test_kafka(transport.TransportCase):
    transport = 'kafka'
    prefix = 'kafka'
    event_loop_max = 100

    def before_connect(self):
        try:
            import kafka  # noqa
        except ImportError:
            raise SkipTest('kafka not installed')

    def after_connect(self, connection):
        connection.channel().client
