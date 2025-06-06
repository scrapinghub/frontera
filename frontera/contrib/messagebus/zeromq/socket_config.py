"""
Contains the SocketConfig class
"""

from socket import gaierror, getaddrinfo


class SocketConfig:
    """
    Converts address to IPv4 or IPv6 or * and returns the necessary socket
    addresses.
    NOTE: When using * it defaults to IPv4
    """

    def __init__(self, address, base_port):
        if address == "*":
            self.ip_addr = "*"
            self.base_port = base_port
            self.is_ipv6 = False
        else:
            try:
                addr_tuple = getaddrinfo(address, base_port)[0][4]
            except gaierror as e:
                raise gaierror(f"Hostname '{address}' could not be resolved") from e
            self.ip_addr = addr_tuple[0]
            self.base_port = addr_tuple[1]
            self.is_ipv6 = len(addr_tuple) == 4

    def spiders_in(self):
        """
        TCP socket for incoming spider messages
        """
        return f"tcp://{self.ip_addr}:{self.base_port}"

    def spiders_out(self):
        """
        TCP socket for outgoing spider messages
        """
        return f"tcp://{self.ip_addr}:{self.base_port + 1}"

    def sw_in(self):
        """
        TCP socket for incoming SW messages
        """
        return f"tcp://{self.ip_addr}:{self.base_port + 2}"

    def sw_out(self):
        """
        TCP socket for outgoing SW messages
        """
        return f"tcp://{self.ip_addr}:{self.base_port + 3}"

    def db_in(self):
        """
        TCP socket for incoming messages
        """
        return f"tcp://{self.ip_addr}:{self.base_port + 4}"

    def db_out(self):
        """
        TCP socket for outgoing DW messages
        """
        return f"tcp://{self.ip_addr}:{self.base_port + 5}"
