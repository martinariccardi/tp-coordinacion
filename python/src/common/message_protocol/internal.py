import json


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))

def serialize_control_msg(client_id):
    return serialize({"client_id": client_id})

def deserialize_control_msg(message):
    return deserialize(message)["client_id"]