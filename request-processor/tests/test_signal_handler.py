import os
import signal
import time
from multiprocessing import Process

from signal_handler import SignalHandler


def test_exit_from_terminate_signal():
    p = _start_process_with_signal_handler()
    p.terminate()
    p.join()
    assert p.is_alive() is False


def test_exit_from_interrupt_signal():
    p = _start_process_with_signal_handler()
    os.kill(p.pid, signal.SIGINT)
    p.join()
    assert p.is_alive() is False


def test_no_exit_from_continue_signal():
    p = _start_process_with_signal_handler()
    os.kill(p.pid, signal.SIGCONT)
    assert p.is_alive() is True
    p.terminate()


def _start_process_with_signal_handler():
    p = Process(target=_run_daemon_using_signal_handler)
    p.start()
    # Allow some time for the process to spawn
    time.sleep(3)
    assert p.is_alive() is True
    return p


def _run_daemon_using_signal_handler():
    signal_handler = SignalHandler()
    while not signal_handler.received_exit_signal:
        pass

