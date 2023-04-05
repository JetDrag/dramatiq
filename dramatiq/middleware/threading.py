# This file is a part of Dramatiq.
#
# Copyright (C) 2017,2018 CLEARTYPE SRL <bogdan@cleartype.io>
#
# Dramatiq is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Dramatiq is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import ctypes
import inspect
import os
import platform
import signal

from ..logging import get_logger

__all__ = ["Interrupt", "raise_thread_exception"]

logger = get_logger(__name__)

current_platform = platform.python_implementation()
python_version = platform.python_version_tuple()
thread_id_ctype = ctypes.c_long if python_version < ("3", "7") else ctypes.c_ulong
supported_platforms = {"CPython"}


def is_gevent_active():
    """Detect if gevent monkey patching is active."""
    try:
        from gevent import monkey
    except ImportError:  # pragma: no cover
        return False
    return bool(monkey.saved)


system_call_interruptable = False
system_call_interrupt_signal = os.getenv('dramatiq_system_call_interrupt_signal', 'SIGUSR1')


def interrupt_signal_handler(signum, frame):
    print('Interrupting system call in worker thread.')
    logger.debug('Interrupting system call in worker thread.')


def enable_system_call_interruptable_support():
    """Support to interrupt system call. Use interruptable signal to interrupt system call
    in thread.
    """
    global system_call_interruptable
    if not system_call_interruptable and hasattr(signal, system_call_interrupt_signal):
        signal.siginterrupt(getattr(signal, system_call_interrupt_signal), True)
        signal.signal(getattr(signal, system_call_interrupt_signal), interrupt_signal_handler)
        system_call_interruptable = True


def disable_system_call_interruptable_support():
    """Disable system call interruptable support."""
    global system_call_interruptable
    if system_call_interruptable:
        signal.siginterrupt(getattr(signal, system_call_interrupt_signal), False)
        signal.signal(getattr(signal, system_call_interrupt_signal), signal.SIG_DFL)
        system_call_interruptable = False


class Interrupt(BaseException):
    """Base class for exceptions used to asynchronously interrupt a
    thread's execution.  An actor may catch these exceptions in order
    to respond gracefully, such as performing any necessary cleanup.

    This is *not* a subclass of ``DramatiqError`` to avoid it being
    caught unintentionally.
    """


def raise_thread_exception(thread_id, exception):
    """Raise an exception in a thread.

    Currently, this is only available on CPython.

    Note:
      This works by setting an async exception in the thread.  This means
      that the exception will only get called the next time that thread
      acquires the GIL.
      If system call interruptable support is enabled, the action will cancel system calls.
    """
    if current_platform == "CPython":
        ret = _raise_thread_exception_cpython(thread_id, exception)
        if ret == 1 and system_call_interruptable:
            try:
                signal.pthread_kill(thread_id, getattr(signal, system_call_interrupt_signal))
            except ProcessLookupError:
                pass
    else:
        message = "Setting thread exceptions (%s) is not supported for your current platform (%r)."
        exctype = (exception if inspect.isclass(exception) else type(exception)).__name__
        logger.critical(message, exctype, current_platform)


def _raise_thread_exception_cpython(thread_id, exception):
    exctype = (exception if inspect.isclass(exception) else type(exception)).__name__
    thread_id = thread_id_ctype(thread_id)
    exception = ctypes.py_object(exception)
    count = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, exception)
    if count == 0:
        logger.critical("Failed to set exception (%s) in thread %r.", exctype, thread_id.value)
    elif count > 1:  # pragma: no cover
        logger.critical("Exception (%s) was set in multiple threads.  Undoing...", exctype)
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.c_long(0))
    return count
