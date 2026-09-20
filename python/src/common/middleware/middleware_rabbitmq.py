import pika
from pika.exceptions import AMQPConnectionError

from .middleware import (
	MessageMiddleware,
	MessageMiddlewareCloseError,
	MessageMiddlewareDisconnectedError,
	MessageMiddlewareMessageError,
	MessageMiddlewareExchange,
	MessageMiddlewareQueue,
)

DISCONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ChannelClosedByBroker,
    pika.exceptions.ConnectionClosed,
    pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.StreamLostError,
)

class MessageMiddlewareBaseRabbitMQ(MessageMiddleware):

	_MSG_ERROR_CLOSED_CONNECTION = "The connection is closed."
	_MSG_ERROR_CLOSED_CHANNEL = "The channel is closed."

	def __init__(self, host):
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
		self.channel = self.connection.channel()
		self.is_consuming = False

	def start_consuming(self, on_message_callback):

		self._verify_connection_is_open()
		self._verify_channel_is_open()
		
		try:
			callback = self._create_callback(on_message_callback)
			self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
		except DISCONNECTION_ERRORS as e:
			raise MessageMiddlewareDisconnectedError() from e
		except Exception as e:
			raise MessageMiddlewareMessageError(e) from e
	
		self.is_consuming = True
		self.channel.start_consuming()

	def stop_consuming(self):
		if not self.is_consuming:
			return
		try:
			self._verify_channel_is_open()
			self.channel.stop_consuming()
		except DISCONNECTION_ERRORS as e:
			raise MessageMiddlewareDisconnectedError() from e
		finally:
			self.is_consuming = False

	def close(self):
		try:
			if self.connection and not self.connection.is_closed:
				self.connection.close()
		except Exception:
			raise MessageMiddlewareCloseError()
		finally:
			self.is_consuming = False

	def _create_callback(self, on_message_callback):
		def callback(ch, method, _properties, body):
			ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
			nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
			on_message_callback(body, ack, nack)
		return callback

	def _verify_connection_is_open(self):
		if not self.connection or self.connection.is_closed:
			raise MessageMiddlewareDisconnectedError(self._MSG_ERROR_CLOSED_CONNECTION)

	def _verify_channel_is_open(self):
		if not self.channel or self.channel.is_closed:
			raise MessageMiddlewareDisconnectedError(self._MSG_ERROR_CLOSED_CHANNEL)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareBaseRabbitMQ, MessageMiddlewareQueue):
	def __init__(self, host, queue_name):
		super().__init__(host)
		self.queue_name = queue_name
		self.channel.queue_declare(queue=self.queue_name)

	def send(self, message):
		try:
			self.channel.basic_publish(exchange="", routing_key=self.queue_name, body=message)
		except DISCONNECTION_ERRORS as e:
			raise MessageMiddlewareDisconnectedError() from e
		except Exception as e:
			raise MessageMiddlewareMessageError(e) from e


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareBaseRabbitMQ, MessageMiddlewareExchange):
	def __init__(self, host, exchange_name, routing_keys):
		super().__init__(host)
		self.exchange_name = exchange_name
		self.routing_keys = routing_keys
		try:
			self._set_up_exchange()
		except DISCONNECTION_ERRORS as e:
			raise MessageMiddlewareDisconnectedError() from e
		except Exception as e:
			raise MessageMiddlewareMessageError(e) from e

	def send(self, message):
		try:
			for key in self.routing_keys:
				self.channel.basic_publish(exchange=self.exchange_name, routing_key=key, body=message)
		except DISCONNECTION_ERRORS as e:
			raise MessageMiddlewareDisconnectedError() from e
		except Exception as e:
			raise MessageMiddlewareMessageError(e) from e

	def _set_up_exchange(self):
		self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="direct")
		
		result = self.channel.queue_declare(queue="", exclusive=True)
		
		self.queue_name = result.method.queue
		
		for key in self.routing_keys:
			self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=key)
		
	


