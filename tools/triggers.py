from datetime import timedelta, datetime
from threading import Thread
import threading
from typing import Any
import inspect

from abc import ABC, abstractmethod

from corvus.tools.time import quantize_time


from typing import Callable


class Event:
    """
        Events are used to create a publish subscriber pattern
    """
    def __init__(self) -> None:
        self.subs = []

    def subscribe(self, func: Callable) -> None:
        """
            Subscribe to this event, when Event.invoke() is called, all subscribed functions are also called with args

        :param func: the callback function that runs when the event is invoked
        """
        self.subs.append(func)

    def unsubscribe(self, func: Callable) -> None:
        """
            Unsubscribe from the event

        :param func: the function to unsubscribe
        """
        self.subs.remove(func)

    def invoke(self, *args) -> None:
        """
            Invoke this event, all subscribers will have their callback functions run

        :param args: the args to back to subscribers
        """
        for func in self.subs:
            func(*args)


class Trigger(ABC):
    def __init__(self) -> None:
        self.on_trigger = Event()
        self.primed = False

        self._invoke = self.on_trigger.invoke
        self.subscribe = self.on_trigger.subscribe
        self.unsubscribe = self.on_trigger.unsubscribe

    def prime(self) -> None:
        self._prime()
        self.primed = True

    def close(self) -> None:
        self._close()
        self.primed = False

    @abstractmethod
    def _prime(self) -> None:
        pass

    @abstractmethod
    def _close(self) -> None:
        pass


class FunctionTrigger(Trigger):
    def __init__(self, pre_prime: bool=True):
        super().__init__()
        self.locked = True

        if pre_prime:
            self.prime()

    def invoke(self, *args) -> None:
        if not self.locked:
            self._invoke(*args)
            print('TRIGGER: Trigger invoked by function call')
        else:
            print("TRIGGER: invoke attempted, but trigger has not been primed")

    def _prime(self) -> None:
        self.locked = False

    def _close(self) -> None:
        self.locked = True


class TimerTrigger(Trigger):
    def __init__(self,
                 duration: timedelta,
                 run_on_start: bool=True,
                 quantize: bool=True,
                 offset: timedelta=timedelta(0)):

        super().__init__()

        if duration <= timedelta(milliseconds=0):
            raise ValueError("duration for TimerTrigger must be > 0")

        self.duration = duration
        self.offset = offset
        self.run_on_start = run_on_start
        self.quantized = quantize

        self.timer = Thread(target=self.tick, name="TimerTrigger Timer")
        self.next_trigger = None

        self.time_format = "%Y/%m/%d %H:%M:%S %f %Z%z"

        self._closing = threading.Event()

    def tick(self) -> None:
        if self.quantized:
            self.next_trigger = quantize_time(self.duration, self.offset, True)
        else:
            self.next_trigger = datetime.utcnow() + self.duration

        if self.run_on_start:
            self._invoke(datetime.utcnow())

        while True:
            sleep_time = (self.next_trigger - datetime.utcnow()).total_seconds()
            if sleep_time < 0:
                raise Exception("Next trigger was set in past ({})".format(self.next_trigger))
            self._closing.wait(sleep_time)

            if self._closing.is_set():
                break

            # next_trigger is actually current_trigger at this point
            self._invoke(self.next_trigger)

            # while loop prevents things from getting backed up if there are problems
            while self.next_trigger <= datetime.utcnow():
                self.next_trigger += self.duration

    def subscribe(self, func: Callable):
        sig = inspect.signature(func)
        arg_count = len(sig.parameters)
        if arg_count != 1:
            msg = "Functions must have 1 argument to subscribe to TimerTrigger, {}() has {}"
            raise TypeError(msg.format(func.__name__, arg_count))

        super().subscribe(func)

    def _prime(self) -> None:
        self.timer.start()

    def _close(self, wait=True) -> None:
        self._closing.set()

        if wait and self.timer.is_alive():
            self.timer.join()

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, TimerTrigger) and \
            self.duration == other.duration and \
            self.offset == other.offset and \
            self.run_on_start == other.run_on_start and \
            self.quantized == other.quantized

    def __hash__(self) -> int:
        return hash(self.duration) + hash(self.offset) + hash(self.run_on_start) + hash(self.quantized) * 5
