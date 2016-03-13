var util = require('util')

var bufferpack = require('bufferpack');
var WebSocketClient = require('websocket').client;


function Client () {

  if (!(this instanceof Client))
      return new Client(source);

  this.client = new WebSocketClient();
}


Client.prototype.rpc_method = function (url, method, blob) {

  this.client.on('connectFailed', function(error) {
      console.log('Connect Error: ' + error.toString());
  });

  this.client.on('connect', function(connection) {
      console.log('WebSocket Client Connected');
      var fmt_str = util.format("<I%ds%ds", method.length, blob.length)
      var buff = bufferpack.pack(fmt_str, [method.length, method, blob])
      connection.sendBytes(buff)
      connection.on('error', function(error) {
          console.log("Connection Error: " + error.toString());
      });
      connection.on('close', function() {
          console.log('echo-protocol Connection Closed');
      });
      connection.on('message', function(message) {
          if (message.type === 'binary') {
              console.log(message.binaryData.toString('utf-8'));
          }
      });
  });


  this.client.connect(url);
}

var client = new Client();
client.rpc_method('http://127.0.0.1:8060', 'echo', 'javascript')
