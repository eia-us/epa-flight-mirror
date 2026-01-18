#!/usr/bin/env python3
"""
Local development server for the EPA GHG API
Runs the Lambda handler locally for testing
"""

import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from handler import lambda_handler

PORT = 4000

class APIHandler(BaseHTTPRequestHandler):
    def _send_response(self, response):
        self.send_response(response['statusCode'])
        for key, value in response.get('headers', {}).items():
            self.send_header(key, value)
        self.end_headers()
        body = response.get('body', '')
        if isinstance(body, str):
            body = body.encode('utf-8')
        self.wfile.write(body)

    def do_OPTIONS(self):
        event = {
            'httpMethod': 'OPTIONS',
            'path': self.path.split('?')[0],
            'body': None
        }
        response = lambda_handler(event, None)
        self._send_response(response)

    def do_GET(self):
        event = {
            'httpMethod': 'GET',
            'path': self.path.split('?')[0],
            'queryStringParameters': self._parse_query_string(),
            'body': None
        }
        response = lambda_handler(event, None)
        self._send_response(response)

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else '{}'

        event = {
            'httpMethod': 'POST',
            'path': self.path.split('?')[0],
            'body': body
        }
        response = lambda_handler(event, None)
        self._send_response(response)

    def _parse_query_string(self):
        if '?' not in self.path:
            return {}
        query = self.path.split('?')[1]
        params = {}
        for param in query.split('&'):
            if '=' in param:
                key, value = param.split('=', 1)
                params[key] = value
        return params

    def log_message(self, format, *args):
        print(f"[{self.log_date_time_string()}] {args[0]}")


def run():
    server = HTTPServer(('localhost', PORT), APIHandler)
    print(f"EPA GHG API running at http://localhost:{PORT}")
    print(f"Example: http://localhost:{PORT}/ghgp/api/version")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()


if __name__ == '__main__':
    run()
