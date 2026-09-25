import os
import logging
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
COUNT_MSG_TYPE = 'count'
EOF_MSG_TYPE = 'eof'
CONTROL_KEY = 'CONTROL_KEY'

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        
        self.control_exchange_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [CONTROL_KEY])
        self.control_exchange_consumer =  middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [CONTROL_KEY]
        )

        self.amount_by_client_by_fruit = {} # {client_id: {fruit: amount}}
        self.expected_total_by_client = {} # {client_id: expected_total}
        self.count_by_client = {} # {client_id: msg_count}
        self.global_count = {} # {client_id: final_count}
        self.lock = threading.Lock()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        client_fruits = self.amount_by_client_by_fruit.setdefault(client_id, {})
        client_fruits[fruit] = client_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

        if client_id in self.expected_total_by_client:
            self._notify_message_count(client_id, 1)   
        else: 
            self.count_by_client[client_id] = self.count_by_client.get(client_id, 0) + 1
           

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        with self.lock:
            if len(fields) == 3:
                self._process_data(*fields)
            else:
                self._notify_eof_to_replicas(*fields)
        ack()


    def _send_to_aggregation(self, client_id):

        if not self._validate_client_count(client_id):
            return 

        client_fruits = self.amount_by_client_by_fruit.pop(client_id, {})

        for final_fruit_item in client_fruits.values():
            target_exchange_id = self._get_aggregation_index(final_fruit_item.fruit)
            output_exchange = self.data_output_exchanges[target_exchange_id]
            output_exchange.send(message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
                                    
        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

        self.expected_total_by_client.pop(client_id, None)
        self.global_count.pop(client_id, None)
        self.count_by_client.pop(client_id, None)

    def _process_control_message(self, message, ack, nack):
        msg_type, client_id, value = message_protocol.internal.deserialize_control_msg(message)
        with self.lock:
            if msg_type == COUNT_MSG_TYPE:
                self.global_count[client_id] = self.global_count.get(client_id, 0) + value
                self._send_to_aggregation(client_id)
            elif msg_type == EOF_MSG_TYPE:
                self.expected_total_by_client[client_id] = value
                processed_items = self.count_by_client.get(client_id, 0)
                if processed_items > 0:
                    self._notify_message_count(client_id, processed_items)
                self._send_to_aggregation(client_id)
        ack()
        
    def start(self):
        control_thread = threading.Thread(target=self._control_message_listener, daemon=True)
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

    def _control_message_listener(self):
        self.control_exchange_consumer.start_consuming(self._process_control_message)

    # REVISAR
    def _get_aggregation_index(self, fruit):
        hash_object = hashlib.md5(fruit.encode())
        return int(hash_object.hexdigest(), 16) % AGGREGATION_AMOUNT

    def _notify_eof_to_replicas(self, client_id, total_messages):
        message = [EOF_MSG_TYPE, client_id, total_messages]
        self.control_exchange_publisher.send(message_protocol.internal.serialize_control_msg(message))

    def _notify_message_count(self, client_id, count):
        message = [COUNT_MSG_TYPE, client_id, count]
        self.control_exchange_publisher.send(message_protocol.internal.serialize_control_msg(message))

    def _validate_client_count(self, client_id):
        confirmed_count = self.global_count.get(client_id,0)  
        expected_count = self.expected_total_by_client.get(client_id,0)
        return client_id in self.expected_total_by_client and confirmed_count == expected_count
        

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
