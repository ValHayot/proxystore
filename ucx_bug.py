import asyncio
import ucp
import numpy as np
import signal
from time import sleep
from multiprocessing import Process
from random import randint

host = "127.0.0.1"
port = randint(5000, 7000)
n_bytes = 2**30

lf = None

async def send(ep):
    global lf
    
    # recv buffer
    arr = np.empty(n_bytes, dtype='u1')
    await ep.recv(arr)
    assert np.count_nonzero(arr) == np.array(0, dtype=np.int64)
    print("Received NumPy array")

    # increment array and send back
    arr += 1
    print("Sending incremented NumPy array")
    await ep.send(arr)

    print("Endpoint closed")
    await ep.close()

def close_listener(*args):
    global lf
    lf.close()
    print('closing')
    assert lf.closed()

def launch_server():
    asyncio.run(server_main())

async def server_main():
    global lf
    lf = ucp.create_listener(send, port)

    # Set the stop condition when receiving SIGINT (ctrl-C) and SIGTERM.
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

    await stop
    close()
    await reset_ucp_async()

def reset_ucp() -> None:  # pragma: no cover
    """Hard reset all of UCP.

    UCP provides :code:`ucp.reset()`; however, this function does not correctly
    shutdown all asyncio tasks and readers. This function wraps
    :code:`ucp.reset()` and additionally removes all readers on the event loop
    and cancels/awaits all asyncio tasks.
    """

    def inner_context() -> None:
        ctx = ucp.core._get_ctx()

        for task in ctx.progress_tasks:
            if task is None:
                continue
            task.event_loop.remove_reader(ctx.epoll_fd)
            if task.asyncio_task is not None:
                try:
                    task.asyncio_task.cancel()
                    task.event_loop.run_until_complete(task.asyncio_task)
                except asyncio.CancelledError:
                    pass

    # We access ucp.core._get_ctx() inside this nested function so our local
    # reference to the UCP context goes out of scope before calling
    # ucp.reset(). ucp.reset() will fail if there are any weak references to
    # to the UCP context because it assumes those may be Listeners or
    # Endpoints that were not properly closed.
    inner_context()

    try:
        ucp.reset()
    except ucp.UCXError:
        pass

async def reset_ucp_async() -> None:  # pragma: no cover
    """Hard reset all of UCP.

    UCP provides :code:`ucp.reset()`; however, this function does not correctly
    shutdown all asyncio tasks and readers. This function wraps
    :code:`ucp.reset()` and additionally removes all readers on the event loop
    and cancels/awaits all asyncio tasks.
    """

    async def inner_context() -> None:
        ctx = ucp.core._get_ctx()

        for task in ctx.progress_tasks:
            if task is None:
                continue
            task.event_loop.remove_reader(ctx.epoll_fd)
            if task.asyncio_task is not None:
                try:
                    task.asyncio_task.cancel()
                    await task.asyncio_task
                except asyncio.CancelledError:
                    pass

    # We access ucp.core._get_ctx() inside this nested function so our local
    # reference to the UCP context goes out of scope before calling
    # ucp.reset(). ucp.reset() will fail if there are any weak references to
    # to the UCP context because it assumes those may be Listeners or
    # Endpoints that were not properly closed.
    await inner_context()

    try:
        ucp.reset()
    except ucp.UCXError:
        pass

def close() -> None:
    global lf
    if lf is not None:
        lf.close()

        while not lf.closed():
            sleep(0.001)

        # Need to lose reference to Listener because UCP does reference
        # counting
        del lf
        lf = None

def server_start():
    asyncio.run(server_main())

async def client_main(proc):
    iterations = randint(1, 9)
    msg = np.zeros(n_bytes, dtype='u1') # create some data to send

    for i in range(iterations):
        ep = await ucp.create_endpoint(host, port)
        # send message
        print("Send Original NumPy array")
        await ep.send(msg)  # send the real message

        # recv response
        print("Receive Incremented NumPy arrays")
        resp = np.empty_like(msg)
        await ep.recv(resp)  # receive the echo
        np.testing.assert_array_equal(msg + 1, resp)

    await ep.close()
    assert ep.closed()
    proc.terminate()
    
async def wait_for_server(host: str, port: int, timeout: float = 5.0) -> None:
    """Wait until the UCXServer responds.

    Args:
        host (str): host of UCXServer to ping.
        port (int): port of UCXServer to ping.
        timeout (float): max time in seconds to wait for server response
            (default: 5.0).
    """
    sleep_time = 0.01
    time_waited = 0.0

    while True:
        try:
            ep = await ucp.create_endpoint(host, port)
        except ucp._libs.exceptions.UCXNotConnected:  # pragma: no cover
            if time_waited >= timeout:
                raise RuntimeError(
                    'Failed to connect to server within timeout '
                    f'({timeout} seconds).',
                )
            await asyncio.sleep(sleep_time)
            time_waited += sleep_time
        else:
            break  # pragma: no cover

    msg = np.zeros(n_bytes, dtype='u1')            
    await ep.send_obj(msg)
    resp = np.empty_like(msg)
    _ = await ep.recv_obj(resp)
    await ep.close()
    assert ep.closed()


if __name__ == "__main__":
    p = Process(target=server_start)
    p.start()
    
    try:
        _loop = asyncio.get_running_loop()
    except RuntimeError:
        _loop = asyncio.new_event_loop()
        
    _loop.run_until_complete(wait_for_server(host=host, port=port))
    _loop.run_until_complete(client_main(p))
