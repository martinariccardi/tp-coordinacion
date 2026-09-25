from common import message_protocol
import uuid


class MessageHandler:

    def __init__(self):
        # TODO: revisar
        self.client_id = uuid.uuid4().int
        self.message_count = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.message_count += 1
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id, self.message_count])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        client_id, fruit_top = fields
        if client_id == self.client_id:
            return fruit_top
        return None
