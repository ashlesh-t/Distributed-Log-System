from fluent import sender


class Logger1:
    def __init__(self, reg=None, heartbeat=None, logs=None):
        self.logger = sender.FluentSender(
            "fluentd.shipping_service", host="localhost", port=9880
        )

        if reg:
            self.send_log("logs", reg)
            print("Registration Log: ", reg)

        if heartbeat:
            self.send_log("heartbeat", heartbeat)
            print("Heartbeat Log: ", heartbeat)

        if logs:
            self.send_log("logs", logs)
            print("Log: ", logs)

    def send_log(self, tag, data):
        try:
            self.logger.emit(tag, data)
        except Exception as e:
            print(f"Failed to send log to Fluentd: {e}")

    def close(self, close_data):
        self.send_log("logs", close_data)
        self.logger.close()
