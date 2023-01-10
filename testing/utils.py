"""Fixtures and utilities for testing."""
from __future__ import annotations

import socket

used_ports = []

def open_port() -> int:
    """Return open port.

    Source: https://stackoverflow.com/questions/2838244
    """
    global used_ports
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    
    if port in used_ports:
        print("port already in use")
        port = open_port()
    else:
        used_ports.append(port)
    return port
