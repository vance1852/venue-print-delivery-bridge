"""核对本地资料端给出的业务输入及线路行为。"""

import http.client
import json
import re
import threading
import time
import unittest
from pathlib import Path

from partner import PartnerServer, load_scenario


class PartnerTest(unittest.TestCase):
    def test_recorded_exchanges(self):
        cases = sorted((Path(__file__).parent / "fixtures").glob("*.json"))
        self.assertGreaterEqual(len(cases), 2)
        for path in cases:
            with self.subTest(case=path.stem):
                self.check_case(load_scenario(path))

    def check_case(self, data):
        server = PartnerServer(("127.0.0.1", 0), data)
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
        worker.start()
        try:
            self.assertTrue(data["facts"])
            self.assertTrue(data["events"])
            self.assertEqual(sorted(event["at"] for event in data["events"]), [event["at"] for event in data["events"]])
            for name in ("facts", "events"):
                status, actual, _ = self.request(server, "GET", "/fixture/" + name)
                self.assertEqual((status, actual), (200, data[name]))

            for exchange in data["exchanges"]:
                path = re.sub(r"\{[A-Za-z_][A-Za-z_0-9]*\}", "recorded-id", exchange["path"])
                args = (server, exchange["method"], path, exchange["match"], exchange.get("request_headers"))
                started = time.monotonic()
                if exchange.get("drop"):
                    with self.assertRaises(http.client.RemoteDisconnected):
                        self.request(*args)
                else:
                    status, actual, headers = self.request(*args)
                    self.assertEqual((status, actual), (exchange["status"], exchange["response"]))
                    for name, value in exchange.get("headers", {}).items():
                        self.assertEqual(headers[name], value)
                self.assertGreaterEqual(time.monotonic() - started + 0.005, exchange.get("delay_ms", 0) / 1000)

            status, journal, _ = self.request(server, "GET", "/fixture/journal")
            self.assertEqual(status, 200)
            self.assertEqual(len(journal), len(data["exchanges"]))
            self.assertTrue(all(item["matched"] for item in journal))
            self.assertEqual(self.request(server, "GET", "/not-in-record")[0], 404)
        finally:
            server.shutdown()
            worker.join(timeout=3)
            server.server_close()

    @staticmethod
    def request(server, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=3)
        try:
            conn.request(method, path, json.dumps(body) if body else None, headers=headers or {})
            response = conn.getresponse()
            payload = response.read()
            return response.status, json.loads(payload) if payload else None, dict(response.getheaders())
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
