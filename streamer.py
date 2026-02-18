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
    def __init__(self, dst_ip, dst_port, src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        # Setup the basic socket stuff
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port

        #keep track of wat we r sending and what we expect to get back
        self.next_send_seq = 0
        self.expected_recv_seq = 0
        self.receive_buffer = {}

        # Pipelining variables so we can send a bunch at once without waiting
        # Key: seq_num, Value: (packet, timestamp)
        self.unacked_packets = {} 
        self.acked = set()
        self.fin_received = False

        # Thread safety is needed because 3 threads will be accessing the same data
        self.lock = threading.Lock()
        self.closed = False

        # We need two background workers, one to listen and one to check for timeouts
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.listener)
        self.executor.submit(self.retransmit_loop)

    #helper function to compute hash
    def compute_hash(self, seq_num, p_type, payload=b''):
        h = hashlib.md5()
        # Hash the header fields AND the data so we know the whole thing is legit
        h.update(struct.pack('!IB', seq_num, p_type))
        h.update(payload)
        return h.digest()

    #helper function to constantly listen to socket
    def listener(self):
        while not self.closed:
            try:
                # If we get a bad or tiny packet, just ignore it
                data, addr = self.socket.recvfrom()
                if not data or len(data) < 21:
                    continue

                # Unpack 21-byte haeder
                seq_num, p_type, received_hash = struct.unpack('!IB16s', data[:21])
                payload = data[21:]

                # Re-calculate hash to see if the network jumbled any bits
                expected_hash = self.compute_hash(seq_num, p_type, payload)
                if received_hash != expected_hash:
                    #hash mishmatch so its corrupted
                    continue

                if p_type == 0: 
                    with self.lock:
                        self.receive_buffer[seq_num] = payload
                    # Send an ACK
                    ack_hash = self.compute_hash(seq_num, 1)
                    ack_packet = struct.pack('!IB16s', seq_num, 1, ack_hash)
                    self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))

                #ACK Packet
                elif p_type == 1:
                    with self.lock:
                        self.acked.add(seq_num)
                        self.unacked_packets.pop(seq_num, None)

                #FIN packet
                elif p_type == 2:
                    with self.lock:
                        self.fin_received = True
                    # ACK the goodbye
                    ack_hash = self.compute_hash(seq_num, 1)
                    ack_packet = struct.pack('!IB16s', seq_num, 1, ack_hash)
                    self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))

            except Exception as e:
                if not self.closed:
                    print(f"Listener error: {e}")
                break

    #helper function to retransmit lost packets
    def retransmit_loop(self):
        while not self.closed:
            try:
                to_retransmit = []
                with self.lock:
                    now = time.time()
                    # Check every packet we've sent but haven't heard back about
                    for seq in list(self.unacked_packets.keys()):
                        packet, send_time = self.unacked_packets[seq]
                        # If it's been more than 0.25s, retransmit
                        if now - send_time > 0.25:
                            to_retransmit.append(packet)
                            # Update timestamp so we don't spam it too fast
                            self.unacked_packets[seq] = (packet, now)
                # Do the actual socket sending outside the lock to keep it fast
                for packet in to_retransmit:
                    self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            except Exception as e:
                if not self.closed:
                    print(f"Retransmit error: {e}")
            time.sleep(0.05)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        MAX_PAYLOAD = 1451  
        for i in range(0, len(data_bytes), MAX_PAYLOAD):
            datachunk = data_bytes[i : i + MAX_PAYLOAD]
            digest = self.compute_hash(self.next_send_seq, 0, datachunk)
            header = struct.pack('!IB16s', self.next_send_seq, 0, digest)
            packet = header + datachunk
            # Throw it onto the network and save it in case it gets lost
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            with self.lock:
                self.unacked_packets[self.next_send_seq] = (packet, time.time())
            self.next_send_seq += 1

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        while True:
            with self.lock:
                # If the specific number we're waiting for is in the buffer, return it
                if self.expected_recv_seq in self.receive_buffer:
                    message = self.receive_buffer.pop(self.expected_recv_seq)
                    self.expected_recv_seq += 1
                    return message
            time.sleep(0.01)

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        #Don't start closing until all data packets are ACKed
        while True:
            with self.lock:
                if not self.unacked_packets:
                    break
            time.sleep(0.01)


        #send out fin and wait for ack
        fin_seq = self.next_send_seq
        fin_hash = self.compute_hash(fin_seq, 2)
        fin_packet = struct.pack('!IB16s', fin_seq, 2, fin_hash)
        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
        with self.lock:
            self.unacked_packets[fin_seq] = (fin_packet, time.time())

        #wait for fin to be acknoledged
        while True:
            with self.lock:
                if fin_seq in self.acked:
                    break
            time.sleep(0.01)

        #wait for fin
        while True:
            with self.lock:
                if self.fin_received:
                    break
            time.sleep(0.01)

        time.sleep(2.0)

        # Cleanup
        self.closed = True
        self.socket.stoprecv()
        self.executor.shutdown(wait=True)