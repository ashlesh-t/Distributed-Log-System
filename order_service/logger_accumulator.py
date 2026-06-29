import fluent.sender
import json


class FluentdLogger:
    def __init__(self, tag="fluentd.order_service", host="localhost", port=9880):

        self.fluentd_logger = fluent.sender.FluentSender(tag, host=host, port=port)

    def add_registration(self, reg):
        self.fluentd_logger.emit("logs", reg)
        print("Registration logged: ", json.dumps(reg, indent=4))

    def add_log(self, log):
        self.fluentd_logger.emit("logs", log)
        print("Log sent to Fluentd: ", json.dumps(log, indent=4))

    def add_heartbeat(self, heartbeat):
        self.fluentd_logger.emit("heartbeat", heartbeat)
        print("Heartbeat sent to Fluentd: ", json.dumps(heartbeat, indent=4))

    def close(self, close_data):
        self.fluentd_logger.emit("logs", close_data)
        self.fluentd_logger.close()
