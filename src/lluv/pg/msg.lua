local ut     = require "lluv.utils"
local struct = require "struct"

local pp = require "pp"

local VER = {
  ["3.0"]     = 196608;
  ["3.0.ssl"] = 80877103;
}

local typ_decode_int = function(n)
  local fmt = '>i' .. n
  return {
    [0] = tonumber;
    [1] = function(data) return struct.unpack(fmt, data) end;
  }
end

local typ_decode_float = function(n)
  return {
    [0] = tonumber;
    [1] = function(data) error('Unsupported binary mode for float types', 3) end;
  }
end

local typ_decode_bool = function()
  local t = typ_decode_int(1)
  local txt, bin = t[0], t[1]
  return {
    [0] = function(...) return txt(...) ~= 0 end;
    [1] = function(...) return bin(...) ~= 0 end;
  }
end

local typ_decode_bin = function()
  local ret = function(data) return data end
  return {[0] = ret; [1] = ret;}
end

local TYPES = {
  [16] = {name = "boolean", ltype = "boolean", len = 1,   decode = typ_decode_bool()};
  [20] = {name = "int8",    ltype = "integer", len = 8,   decode = typ_decode_int(8)};
  [21] = {name = "int2",    ltype = "integer", len = 2,   decode = typ_decode_int(2)};
  [23] = {name = "int4",    ltype = "integer", len = 4,   decode = typ_decode_int(4)};

  [700] = {name = "float4", ltype = "number",  len = 4,   decode = typ_decode_float(4)};
  [701] = {name = "float8", ltype = "number",  len = 8,   decode = typ_decode_float(8)};

  [19] = {name = "name",    ltype = "string",  len = 64,  decode = typ_decode_bin() };
  [25] = {name = "text",    ltype = "string",  len = nil, decode = typ_decode_bin() };
}

local function DecodeValue(value, mode, tid)
  local typ = TYPES[tid]
  if not typ then return value end
  return typ.decode[mode](data), typ.name
end

local function DecodeType(tid)
  local typ = TYPES[tid]
  if not typ then return 'string' end
  return typ.ltype, typ.name
end

local MessageEncoder = {} do

local Frontend = {
  Bind                            = 'B';
  Close                           = 'C';
  CopyFail                        = 'f';
  Describe                        = 'D';
  Execute                         = 'E';
  Flush                           = 'H';
  FunctionCall                    = 'F';
  Parse                           = 'P';
  PasswordMessage                 = 'p';
  Query                           = 'Q';
  Sync                            = 'S';
  Terminate                       = 'X';
  CopyData                        = 'd';
  CopyDone                        = 'c';
}

local function pack(typ, data)
  local header = struct.pack(">c1i4", typ, #data + 4)
  return header, data
end

MessageEncoder.greet = function(ver, opt)
  ver = assert(VER[ver])
  local str = ''
  if opt then
    for k, v in pairs(opt) do
      str = str .. k .. '\0' .. v .. '\0'
    end
  end
  str = str .. '\0'
  return struct.pack(">I4I4", #str + 8, ver), str
end

MessageEncoder.Query = function(sql)
  return pack('Q', sql .. '\0')
end

MessageEncoder.PasswordMessage = function(msg)
  return pack('p', msg)
end

MessageEncoder.Terminate = function()
  return pack("X", "")
end

MessageEncoder.CancelRequest = function(pid, key)
  local magic = 80877102
  return pack("F", struct.pack(">I4I4I4", magic, pid, key))
end

end

local MessageDecoder = {} do

MessageDecoder.dispatch = function(typ, msg)
  return MessageDecoder[typ](typ, msg)
end

local Backend = {
  AuthenticationOk                = 'R';
  AuthenticationKerberosV5        = 'R';
  AuthenticationCleartextPassword = 'R';
  AuthenticationMD5Password       = 'R';
  AuthenticationSCMCredential     = 'R';
  AuthenticationGSS               = 'R';
  AuthenticationSSPI              = 'R';
  AuthenticationGSSContinue       = 'R';
  BackendKeyData                  = 'K';
  BindComplete                    = '2';
  CloseComplete                   = '3';
  CommandComplete                 = 'C';
  CopyInResponse                  = 'G';
  CopyOutResponse                 = 'H';
  CopyBothResponse                = 'W';
  DataRow                         = 'D';
  EmptyQueryResponse              = 'I';
  ErrorResponse                   = 'E';
  FunctionCallResponse            = 'V';
  NoData                          = 'n';
  NoticeResponse                  = 'N';
  NotificationResponse            = 'A';
  ParameterDescription            = 't';
  ParameterStatus                 = 'S';
  ParseComplete                   = '1';
  PortalSuspended                 = 's';
  ReadyForQuery                   = 'Z';
  RowDescription                  = 'T';
  CopyData                        = 'd';
  CopyDone                        = 'c';
}

for k, v in pairs(Backend) do
  MessageDecoder[v] = function(typ, msg) return k, msg end
end

MessageDecoder['R'] = function(typ, msg)
  assert(#msg >= 4)
  local b = struct.unpack(">I4", msg)
  if b == 0 then return "AuthenticationOk",                msg end
  if b == 2 then return "AuthenticationKerberosV5",        msg end
  if b == 3 then return "AuthenticationCleartextPassword", msg end
  if b == 5 then return "AuthenticationMD5Password",       msg end
  if b == 6 then return "AuthenticationSCMCredential",     msg end
  if b == 7 then return "AuthenticationGSS",               msg end
  if b == 9 then return "AuthenticationSSPI",              msg end
  if b == 8 then return "AuthenticationGSSContinue",       msg end
  assert(false, "Unknown auth message: " .. tostring(a) .. "/" .. tostring(b))
end

function MessageDecoder.ErrorResponse(data)
  local typ, field, pos
  local res = {}
  if #data > 0 then while true do
    typ, field, pos = struct.unpack('c1s', data, pos)
    if not typ then return on_protocol_error('E', data) end
    res[typ] = field
    if pos == #data then break end
  end end
  assert(res.S, "Severity required")
  assert(res.C, "SQLSTATE required")
  assert(res.M, "Message  required")
  return res
end

function MessageDecoder.AuthenticationMD5Password(data)
  assert(#data == 8)
  local mode, salt = struct.unpack(">I4c4", data)
  assert(mode == 5)
  return mode, salt
end

function MessageDecoder.AuthenticationCleartextPassword(data)
  assert(#data == 4)
  local mode = struct.unpack(">I4", data)
  assert(mode == 3)

  return mode
end

function MessageDecoder.ParameterStatus(data)
  local key, value = struct.unpack("ss", data)
  return key, value
end

function MessageDecoder.BackendKeyData(data)
  assert(#data >= 8)
  local pid, key = struct.unpack(">I4I4", data)
  return pid, key
end

function MessageDecoder.RowDescription(data)
  local n, pos = struct.unpack(">I2", data)
  local rows = {}
  for i = 1, n do
    local name, tid, cid, ftyp, typlen, typmod, fcode
    name, tid, cid, ftyp, typlen, typmod, fcode, pos = struct.unpack(">sI4I2I4I2I4I2", data, pos)
    if not name then break end
    rows[#rows + 1] = { name, ftyp, fcode, typlen, typmod }
  end
  assert(n == #rows)
  return rows, n
end

function MessageDecoder.DataRow(data)
  local n, pos = struct.unpack(">I2", data)

  local row = {}
  for i = 1, n do
    local len len, pos = struct.unpack(">i4", data, pos)
    if len < 0 then  row[i] = nil
    else
      if len == 0 then
        row[i] = ''
      else
        row[i] = string.sub(data, pos, pos + len - 1)
        pos = pos + len
      end
    end
  end

  return row
end

function MessageDecoder.CommandComplete(data)
  local a, b, c = string.match(data, "^([A-Za-z]+)%s(%d+)%s?(%d*)")
  b,c = tonumber(b), tonumber(c)
  if c then
    assert(a == 'INSERT')
    return a, c, b
  end
  return a, b
end

function MessageDecoder.NoticeResponse(data)
  local res = {}
  local b, pos = struct.unpack('>I1', data)
  while b ~= 0 do
    local val val, pos = struct.unpack('>I1', data, pos)
    res[#res + 1] = res
    b, pos = struct.unpack('>I1', data, pos)
  end
  return res
end

function MessageDecoder.NotificationResponse(data)
  local pid, name, payload = struct.unpack('>I4ss', data)
  return pid, name, payload
end

end

return {
  decoder = MessageDecoder;
  encoder = MessageEncoder;
  decode_value = DecodeValue;
  decode_type  = DecodeType;
}
