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

        # Part 5: track individual unacked packets for pipelining
        self.unacked_packets = {}  # seq -> (packet_bytes, send_time)
        self.acked = set()
        self.fin_received = False

        self.lock = threading.Lock()
        self.closed = False
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.executor.submit(self.listener)
        self.executor.submit(self._retransmit_loop)

    def _compute_hash(self, seq_num, p_type, payload=b''):
        """Compute MD5 hash over header fields + payload (not the hash field itself)."""
        h = hashlib.md5()
        h.update(struct.pack('!IB', seq_num, p_type))
        h.update(payload)
        return h.digest()

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                if not data or len(data) < 21:
                    continue

                seq_num, p_type, received_hash = struct.unpack('!IB16s', data[:21])
                payload = data[21:]

                # Verify hash for ALL packet types
                expected_hash = self._compute_hash(seq_num, p_type, payload)
                if received_hash != expected_hash:
                    continue

                if p_type == 0:  # DATA
                    with self.lock:
                        self.receive_buffer[seq_num] = payload
                    # Send ACK (with proper hash)
                    ack_hash = self._compute_hash(seq_num, 1)
                    ack_packet = struct.pack('!IB16s', seq_num, 1, ack_hash)
                    self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))

                elif p_type == 1:  # ACK
                    with self.lock:
                        self.acked.add(seq_num)
                        self.unacked_packets.pop(seq_num, None)

                elif p_type == 2:  # FIN
                    with self.lock:
                        self.fin_received = True
                    # ACK the FIN (with proper hash)
                    ack_hash = self._compute_hash(seq_num, 1)
                    ack_packet = struct.pack('!IB16s', seq_num, 1, ack_hash)
                    self.socket.sendto(ack_packet, (self.dst_ip, self.dst_port))

            except Exception as e:
                if not self.closed:
                    print(f"Listener error: {e}")
                break

    def _retransmit_loop(self):
        """Background thread that retransmits unacked packets after timeout."""
        while not self.closed:
            try:
                to_retransmit = []
                with self.lock:
                    now = time.time()
                    for seq in list(self.unacked_packets.keys()):
                        packet, send_time = self.unacked_packets[seq]
                        if now - send_time > 0.25:
                            to_retransmit.append(packet)
                            self.unacked_packets[seq] = (packet, now)
                for packet in to_retransmit:
                    self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            except Exception as e:
                if not self.closed:
                    print(f"Retransmit error: {e}")
            time.sleep(0.05)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        MAX_PAYLOAD = 1451  # 1472 - 21 byte header
        for i in range(0, len(data_bytes), MAX_PAYLOAD):
            datachunk = data_bytes[i : i + MAX_PAYLOAD]
            digest = self._compute_hash(self.next_send_seq, 0, datachunk)
            header = struct.pack('!IB16s', self.next_send_seq, 0, digest)
            packet = header + datachunk

            # Send without waiting for ACK (pipelining)
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            with self.lock:
                self.unacked_packets[self.next_send_seq] = (packet, time.time())
            self.next_send_seq += 1

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        while True:
            with self.lock:
                if self.expected_recv_seq in self.receive_buffer:
                    message = self.receive_buffer.pop(self.expected_recv_seq)
                    self.expected_recv_seq += 1
                    return message
            time.sleep(0.01)

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # 1. Wait for all in-flight data packets to be ACKed
        while True:
            with self.lock:
                if not self.unacked_packets:
                    break
            time.sleep(0.01)

        # 2. Send FIN and track it for retransmission
        fin_seq = self.next_send_seq
        fin_hash = self._compute_hash(fin_seq, 2)
        fin_packet = struct.pack('!IB16s', fin_seq, 2, fin_hash)
        self.socket.sendto(fin_packet, (self.dst_ip, self.dst_port))
        with self.lock:
            self.unacked_packets[fin_seq] = (fin_packet, time.time())

        # 3. Wait for FIN ACK (retransmit loop handles retransmission)
        while True:
            with self.lock:
                if fin_seq in self.acked:
                    break
            time.sleep(0.01)

        # 4. Wait for FIN from other side
        while True:
            with self.lock:
                if self.fin_received:
                    break
            time.sleep(0.01)

        # 5. Grace period — keep listener alive to re-ACK retransmitted FINs
        time.sleep(2.0)

        # 6. Cleanup
        self.closed = True
        self.socket.stoprecv()
        self.executor.shutdown(wait=True)
