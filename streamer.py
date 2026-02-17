# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        self.next_send_seq = 0
        self.expected_recv_seq = 0  
        self.receive_buffer = {}

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        MAX_PAYLOAD = 1468
    # 2. Loop through the data_bytes using slicing
        for i in range(0, len(data_bytes), MAX_PAYLOAD):
            datachunk = data_bytes[i : i + MAX_PAYLOAD]
            header = struct.pack('!I', self.next_send_seq)
            packet = header + datachunk
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            self.next_send_seq += 1

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        
        while self.expected_recv_seq not in self.receive_buffer:
            # Waiting for packet to arrive
            data, addr = self.socket.recvfrom()
            
            # Unpack  first 4 bytes to get seqeunce number
            header = data[:4]
            seq_num = struct.unpack('!I', header)[0]
            payload = data[4:]
            
            # Store it in waiting area
            self.receive_buffer[seq_num] = payload

        # Once this line is reached, packet is in buffer
        message = self.receive_buffer.pop(self.expected_recv_seq)
        self.expected_recv_seq += 1
        return message

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
