import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.received_eofs_by_client = {}

        self.closed = False

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.closed = True    
        self.input_exchange.stop_consuming()
        
        
    def disconnect(self):
        try:
            self.input_exchange.close()
            self.output_queue.close()
        except Exception:
            logging.error("Error while disconnecting middleware")

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        client_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(client_top)):
            if client_top[i].fruit == fruit:
                updated = client_top.pop(i) + fruit_item.FruitItem(
                    fruit, amount
                )
                bisect.insort(client_top, updated)
                return
        bisect.insort(client_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        count = self.received_eofs_by_client.get(client_id, 0) + 1
        self.received_eofs_by_client[client_id] = count
        if count < SUM_AMOUNT:
            logging.info("HOLA")
            return
        del self.received_eofs_by_client[client_id]
        self._send_final_fruit_top(client_id)

    def _send_final_fruit_top(self, client_id):
        client_top = self.fruit_top_by_client.pop(client_id, {})
        fruit_chunk = list(client_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top_by_client = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk
            )
        )
        self.output_queue.send(message_protocol.internal.serialize([client_id, fruit_top_by_client]))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        try:
            self.input_exchange.start_consuming(self.process_messsage)
        except Exception:
            logging.exception("Error while consuming messages")
        finally:
            self.disconnect()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
