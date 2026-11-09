"""A minimal Bolt server: enough of the protocol for the real neo4j driver to authenticate, run and fail queries."""

import socket
import struct
import threading


def pstr(s: str) -> bytes:
    b = s.encode()
    if len(b) < 16:
        return bytes([0x80 | len(b)]) + b
    if len(b) < 256:
        return bytes([0xD0, len(b)]) + b
    return bytes([0xD1]) + struct.pack(">H", len(b)) + b


def pint(i: int) -> bytes:
    if 0 <= i < 128:
        return bytes([i])
    return bytes([0xC9]) + struct.pack(">h", i)


def pval(v) -> bytes:
    if isinstance(v, bool):
        return b"\xc3" if v else b"\xc2"
    if v is None:
        return b"\xc0"
    if isinstance(v, int):
        return pint(v)
    if isinstance(v, str):
        return pstr(v)
    if isinstance(v, list):
        return bytes([0x90 | len(v)]) + b"".join(pval(x) for x in v)
    if isinstance(v, dict):
        return bytes([0xA0 | len(v)]) + b"".join(pstr(k) + pval(x) for k, x in v.items())
    raise TypeError(v)


def struct_msg(tag: int, *fields) -> bytes:
    body = bytes([0xB0 | len(fields), tag]) + b"".join(pval(f) for f in fields)
    return struct.pack(">H", len(body)) + body + b"\x00\x00"


def read_exact(conn, n):
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise EOFError
        buf += chunk
    return buf


def read_message(conn) -> bytes:
    data = b""
    while True:
        size = struct.unpack(">H", read_exact(conn, 2))[0]
        if size == 0:
            if data:
                return data
            continue
        data += read_exact(conn, size)


def query_of(msg: bytes) -> str:
    # RUN: B3 10 <string query> ...
    i = 2
    marker = msg[i]
    if marker & 0xF0 == 0x80:
        n, i = marker & 0x0F, i + 1
    elif marker == 0xD0:
        n, i = msg[i + 1], i + 2
    else:
        n, i = struct.unpack(">H", msg[i + 1 : i + 3])[0], i + 3
    return msg[i : i + n].decode()


class FakeBolt:
    def __init__(self, good_password="good"):
        self.good_password = good_password
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.port = self.sock.getsockname()[1]
        self.queries = []
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        try:
            read_exact(conn, 20)
            conn.sendall(bytes([0, 0, 4, 5]))
            pending_failure = False
            while True:
                msg = read_message(conn)
                tag = msg[1]
                if tag == 0x01:  # HELLO
                    conn.sendall(struct_msg(0x70, {"server": "Neo4j/5.26.0", "connection_id": "bolt-1"}))
                elif tag == 0x6A:  # LOGON
                    if self.good_password.encode() in msg:
                        conn.sendall(struct_msg(0x70, {}))
                    else:
                        conn.sendall(
                            struct_msg(
                                0x7F,
                                {
                                    "code": "Neo.ClientError.Security.Unauthorized",
                                    "message": "The client is unauthorized due to authentication failure.",
                                },
                            )
                        )
                        return
                elif tag in (0x54, 0x6B):  # TELEMETRY, LOGOFF
                    conn.sendall(struct_msg(0x70, {}))
                elif tag == 0x10:  # RUN
                    q = query_of(msg)
                    self.queries.append(q)
                    if "READONLY" in q:
                        conn.sendall(
                            struct_msg(
                                0x7F,
                                {
                                    "code": "Neo.ClientError.Statement.AccessMode",
                                    "message": "Writing in read access mode not allowed. Attempted write to neo4j",
                                },
                            )
                        )
                        pending_failure = True
                    elif "BAD" in q:
                        conn.sendall(
                            struct_msg(
                                0x7F,
                                {
                                    "code": "Neo.ClientError.Statement.SyntaxError",
                                    "message": "Invalid input 'BAD': expected 'MATCH' (line 1, column 1 (offset: 0))\n\"BAD query\"\n ^",
                                },
                            )
                        )
                        pending_failure = True
                    else:
                        fields = ["test"] if q.startswith("RETURN 1") else []
                        conn.sendall(struct_msg(0x70, {"fields": fields, "t_first": 0, "qid": 0}))
                elif tag == 0x3F:  # PULL
                    if pending_failure:
                        conn.sendall(struct_msg(0x7E))
                    elif "STREAMFAIL" in self.queries[-1]:
                        conn.sendall(
                            struct_msg(
                                0x7F,
                                {
                                    "code": "Neo.ClientError.Statement.ArithmeticError",
                                    "message": "/ by zero",
                                },
                            )
                        )
                        pending_failure = True
                    else:
                        q = self.queries[-1]
                        if q.startswith("RETURN 1"):
                            conn.sendall(struct_msg(0x71, [1]))
                        conn.sendall(
                            struct_msg(
                                0x70,
                                {
                                    "type": "w" if q.startswith("CREATE") else "r",
                                    "t_last": 0,
                                    "db": "neo4j",
                                    "stats": {"nodes-created": 1} if q.startswith("CREATE") else {},
                                },
                            )
                        )
                elif tag == 0x0F:  # RESET
                    pending_failure = False
                    conn.sendall(struct_msg(0x70, {}))
                elif tag == 0x02:  # GOODBYE
                    return
                else:
                    conn.sendall(struct_msg(0x70, {}))
        except (EOFError, OSError):
            pass
        finally:
            conn.close()
