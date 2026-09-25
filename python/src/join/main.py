import os
import logging
import bisect
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.tops_received_by_client = {}
        self.tops_count_by_client = {}

        self.closed = False

    def handle_sigterm(self, signum, frame):
        logging.info("Received SIGTERM signal")
        self.closed = True    
        self.input_queue.stop_consuming()
            
            
    def disconnect(self):
        try:
            self.input_queue.close()
            self.output_queue.close()
        except Exception:
            logging.error("Error while disconnecting middleware")

    def process_messsage(self, message, ack, nack):
        logging.info("Received partial top")
        client_id, partial_fruit_top = message_protocol.internal.deserialize(message)

        client_top = self.tops_received_by_client.setdefault(client_id, [])
        for fruit, amount in partial_fruit_top:
            bisect.insort(client_top, fruit_item.FruitItem(fruit, amount))

        count = self.tops_count_by_client.get(client_id, 0) + 1
        self.tops_count_by_client[client_id] = count

        if self.tops_count_by_client[client_id] < AGGREGATION_AMOUNT:
            return

        del self.tops_count_by_client[client_id]
        client_top = self.tops_received_by_client.pop(client_id)
        self._send_final_top(client_id, client_top)
        ack()


    def _send_final_top(self, client_id, client_top):
        top_chunk = client_top[-TOP_SIZE:]
        top_chunk.reverse()
        final_fruit_top = [(item.fruit, item.amount) for item in top_chunk]

        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_fruit_top])
        )


    def start(self):
        signal.signal(signal.SIGTERM, self.handle_sigterm)
        try:
            self.input_queue.start_consuming(self.process_messsage)
        except Exception:
            logging.exception("Error while consuming messages")
        finally:
            self.disconnect()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
