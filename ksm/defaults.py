class Defaults(object):
    """Default parameters for a typical installation"""

    # host and port of the message manager (Kafka broker)
    KAFKA_HOST = "localhost"
    KAFKA_PORT = 9092
    KAFKA_TOPIC = ["test"]
    KAFKA_LOOP_DELAY = 0.001
