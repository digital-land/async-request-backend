from signal import SIGINT, SIGTERM, signal


class SignalHandler:
    def __init__(self):
        self.received_exit_signal = False
        signal(SIGINT, self._exit_signal_handler)
        signal(SIGTERM, self._exit_signal_handler)

    def _exit_signal_handler(self, signal, frame):
        print(f"handling signal {signal}, exiting gracefully")
        self.received_exit_signal = True
