"""本地合作方资料与 HTTP 交换记录。"""

import argparse
import json
import re
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


def load_scenario(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def path_pattern(value):
    parts = value.split("/")
    return "/".join(
        r"[^/]+" if re.fullmatch(r"\{[A-Za-z_][A-Za-z_0-9]*\}", part) else re.escape(part)
        for part in parts
    )


class PartnerServer(ThreadingHTTPServer):
    def __init__(self, address, scenario):
        super().__init__(address, PartnerHandler)
        self.scenario = scenario
        self.remaining = list(scenario["exchanges"])
        self.journal = []
        self.lock = threading.Lock()

    def consume(self, method, path, body, headers):
        with self.lock:
            for index, exchange in enumerate(self.remaining):
                if (
                    exchange["method"] == method
                    and re.fullmatch(path_pattern(exchange["path"]), path)
                    and all(key in body and body[key] == value for key, value in exchange["match"].items())
                    and all(headers.get(key) == value for key, value in exchange.get("request_headers", {}).items())
                ):
                    break
            else:
                exchange = {"status": 404, "response": {"message": "记录中没有匹配的交换"}}
                index = None
            if index is not None:
                del self.remaining[index]
            self.journal.append({
                "method": method,
                "path": path,
                "body": body,
                "status": exchange["status"],
                "disconnected": exchange.get("drop", False),
                "matched": index is not None,
            })
            return exchange


class PartnerHandler(BaseHTTPRequestHandler):
    def reply(self, status, value, headers=None):
        payload = b"" if status in (204, 304) else json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        if payload:
            self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def dispatch(self):
        path = urlsplit(self.path).path
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 <= length <= 262144:
                raise ValueError("资料超出限制")
            body = json.loads(self.rfile.read(length)) if length else {}
            if not isinstance(body, dict):
                raise ValueError("请求必须是 JSON 对象")
        except (ValueError, UnicodeError):
            self.reply(400, {"message": "无法读取请求正文"})
            return

        if self.command == "GET" and path in ("/fixture/facts", "/fixture/events", "/fixture/journal"):
            key = path.rsplit("/", 1)[-1]
            if key == "journal":
                with self.server.lock:
                    value = list(self.server.journal)
            else:
                value = self.server.scenario[key]
            self.reply(200, value)
            return

        exchange = self.server.consume(self.command, path, body, self.headers)
        time.sleep(exchange.get("delay_ms", 0) / 1000)
        if exchange.get("drop"):
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_WR)
            return
        try:
            self.reply(exchange["status"], exchange["response"], exchange.get("headers"))
        except (BrokenPipeError, ConnectionResetError):
            return

    do_GET = dispatch
    do_POST = dispatch
    do_PUT = dispatch

    def log_message(self, *args):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", type=Path, help="合作方资料文件")
    parser.add_argument("--port", type=int, default=8090)
    args = parser.parse_args()
    with PartnerServer(("127.0.0.1", args.port), load_scenario(args.scenario)) as server:
        print(f"合作方接口 http://127.0.0.1:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
