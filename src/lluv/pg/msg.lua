local ut     = require "lluv.utils"
local struct = require "lluv.pg.utils.bin"
local NULL   = require "null".null

local unpack = unpack or table.unpack

local function append(t, v) t[#t+1] = v return t end

local VER = {
  ["3.0"]     = 196608;
  ["3.0.ssl"] = 80877103;
}

local function read_data(data, pos)
  local len len, pos = struct.unpack(">i4", data, pos)
  if len < 0 then return NULL, pos end
  if len == 0 then return '', pos end
  return string.sub(data, pos, pos + len - 1), pos + len
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

MessageEncoder.Parse = function(name, sql, types)
  local data = name .. '\0' .. sql .. '\0'
  if types then
    data = data .. struct.pack(
      ">I2" .. ("I4"):rep(#types),
      #types, unpack(types)
    )
  else
    data = data .. struct.pack(">I2", 0)
  end

  return pack("P", data)
end

MessageEncoder.Describe = function(typ, name)
  assert(typ == 'S' or typ == 'P')
  return pack('D', typ .. name .. '\0')
end

MessageEncoder.Sync = function(name, value)
  return pack("S", "")
end

MessageEncoder.Bind = function(portal, name, formats, values, result_formats)
  local buf = {portal .. '\0' .. name .. '\0'}

  if formats then -- binary or text
    append(buf, struct.pack(">I2", #formats))
    for i = 1, #formats do
      append(buf, struct.pack(">I2", formats[i]))
    end
  else
    append(buf, struct.pack(">I2", 0))
  end

  if values then
    append(buf, struct.pack(">I2", #values))
    for i = 1, #values do
      v = values[i]
      if v == NULL then
        append(buf, struct.pack(">i4", -1))
      else
        assert(type(v) == 'string')
        append(buf, struct.pack(">I4", #v) .. v)
      end
    end
  else
    append(buf, struct.pack(">I2", 0))
  end

  if result_formats then -- binary or text
    append(buf, struct.pack(">I2", #result_formats))
    for i = 1, #result_formats do
      append(buf, struct.pack(">I2", result_formats[i]))
    end
  else
    append(buf, struct.pack(">I2", 0))
  end

  return pack('B', table.concat(buf))
end

MessageEncoder.Execute = function(portal, rows)
  local data = portal .. '\0' .. struct.pack(">I4", rows or 0)
  return pack('E', data)
end

MessageEncoder.Close = function(typ, name)
  assert(typ == 'S' or typ == 'P')
  return pack('C', typ .. name .. '\0')
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

function MessageDecoder.ReadyForQuery(data)
  assert(#data == 1)
  return data
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
    row[i], pos = read_data(data, pos)
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

function MessageDecoder.ParseComplete(data)
  assert(#data == 0)
  return data
end

function MessageDecoder.PortalSuspended(data)
  assert(#data == 0)
  return data
end

function MessageDecoder.ParameterDescription(data)
  local n, pos = struct.unpack(">I2", data)
  local t = {}
  for i = 1, n do
    local typ typ, pos = struct.unpack(">I2", data, pos)
    append(t, typ)
  end
  return t
end

end

return {
  decoder   = MessageDecoder;
  encoder   = MessageEncoder;
  NULL      = NULL;
}
