# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct

from concurrent.futures import ThreadPoolExecutor
import threading
import time
import hashlib


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

        self.lock = threading.Lock()
        self.closed = False
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.executor.submit(self.listener)
        self.acked_seq = -1

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                if not data or len(data) < 21:
                    continue
                header = struct.unpack('!IB16s', data[:21])
                seq_num = header[0]
                p_type = header[1]
                received_hash = header[2]
                payload = data[21:]

                if p_type == 0:
                    h = hashlib.md5()
                    h.update(payload)
                    if h.digest() != received_hash:
                        print("Corrupted!")
                        continue 

                    with self.lock:
                        self.receive_buffer[seq_num] = payload
                        ack_packet = struct.pack('!IB16s', seq_num, 1, b'\x00'*16)
                        self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))
                
                elif p_type == 1: 
                    with self.lock:
                        self.acked_seq = max(self.acked_seq, seq_num)

                elif p_type == 2: 
                    ack_packet = struct.pack('!IB16s', seq_num, 1, b'\x00'*16)
                    self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))

            except Exception as e:
                if not self.closed:
                    print(f"Listener error: {e}")
                break

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        MAX_PAYLOAD = 1451
        for i in range(0, len(data_bytes), MAX_PAYLOAD):
            datachunk = data_bytes[i : i + MAX_PAYLOAD]
            h = hashlib.md5()
            h.update(datachunk)
            digest = h.digest() 
            
            header = struct.pack('!IB16s', self.next_send_seq, 0, digest)
            packet = header + datachunk
            
            acked = False
            while not acked:
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                
                start_time = time.time()
                while time.time() - start_time < 0.25:
                    with self.lock:
                        if self.acked_seq >= self.next_send_seq:
                            acked = True
                            break
                    time.sleep(0.01)
                
                if acked:
                    break
                else:
                    print(f"Timeout! Resending packet {self.next_send_seq}")
            
            self.next_send_seq += 1

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        
        import time 
        while True:
            with self.lock:
                if self.expected_recv_seq in self.receive_buffer:
                    message = self.receive_buffer.pop(self.expected_recv_seq)
                    self.expected_recv_seq += 1
                    return message
            #print("Still waiting for packet")
            time.sleep(0.01)

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        while True:
            with self.lock:
                if self.acked_seq >= self.next_send_seq - 1:
                    break
            time.sleep(0.01)

        # 2. Send FIN (Type 2) and wait for its ACK
        # We reuse the same logic as send()
        fin_packet = struct.pack('!IB16s', self.next_send_seq, 2, b'\x00'*16)
        acked = False
        while not acked:
            self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
            start_time = time.time()
            while time.time() - start_time < 0.25:
                with self.lock:
                    if self.acked_seq >= self.next_send_seq:
                        acked = True
                        break
                time.sleep(0.01)
        
        # 3. Give the other side a moment to receive our last ACK
        time.sleep(2.0)
        
        self.closed = True
        self.socket.stoprecv()
        self.executor.shutdown(wait=True)