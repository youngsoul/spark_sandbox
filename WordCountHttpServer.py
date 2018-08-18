#!/usr/bin/python
from http.server import BaseHTTPRequestHandler, HTTPServer
from os import path
import os

PORT_NUMBER = 8080

d = path.dirname(__file__) if "__file__" in locals() else os.getcwd()

# This class will handles any incoming request from
# the browser
class myHandler(BaseHTTPRequestHandler):

    def load_binary(self, file):
        return open(file, 'rb')


    # Handler for the GET requests
    def do_GET(self):
        if self.path == "/":
            self.path = "./data/tweet_wc.png"

        try:
            file = d + '/' + self.path
            # Check the file extension required and
            # set the right mime type

            sendReply = False
            if self.path.endswith(".html"):
                mimetype = 'text/html'
                sendReply = True
                f = open(file)
            if self.path.endswith(".png"):
                mimetype = 'image/png'
                sendReply = True
                f = self.load_binary(file)
            if self.path.endswith(".jpg"):
                mimetype = 'image/jpg'
                sendReply = True
                f = self.load_binary(file)
            if self.path.endswith(".gif"):
                mimetype = 'image/gif'
                sendReply = True
                f = self.load_binary(file)
            if self.path.endswith(".js"):
                mimetype = 'application/javascript'
                f = open(file)
                sendReply = True
            if self.path.endswith(".css"):
                mimetype = 'text/css'
                f = open(file)
                sendReply = True

            if sendReply == True:
                # Open the static file requested and send it
                self.send_response(200)
                self.send_header('Content-type', mimetype)
                self.end_headers()
                self.wfile.write(f.read())
                f.close()
            return


        except IOError:
            self.send_error(404, 'File Not Found: %s' % self.path)


try:
    # Create a web server and define the handler to manage the
    # incoming request
    server = HTTPServer(('', PORT_NUMBER), myHandler)
    print
    'Started httpserver on port ', PORT_NUMBER

    # Wait forever for incoming htto requests
    server.serve_forever()

except KeyboardInterrupt:
    print
    '^C received, shutting down the web server'
    server.socket.close()