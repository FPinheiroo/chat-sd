const protobuf = require("protobufjs");

const root = protobuf.loadSync(__dirname + "/chat.proto");
const Msg = root.lookupType("chat.Msg");

function codificar(obj) {
  return Msg.encode(Msg.fromObject(obj)).finish();
}

function decodificar(buffer) {
  return Msg.toObject(Msg.decode(buffer), { defaults: true, arrays: true });
}

module.exports = { codificar, decodificar };
