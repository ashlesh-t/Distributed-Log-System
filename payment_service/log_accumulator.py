from fluent import sender
from fluent import event

sender.setup("fluentd.payment_service", host="localhost", port=9880)


class Logger1:
    def __init__(self, reg=None, logs=None, heartbeat=None):
        self.logs = logs
        self.heartbeat = heartbeat
        if reg:
            event.Event("logs", reg)
        if self.logs:
            print(logs)
            event.Event("logs", logs)

        if self.heartbeat:
            event.Event("heartbeat", heartbeat)

    def close(self, close_log):
        event.Event("logs", close_log)
        sender.close()
