local md5            = require "md5"
local ut             = require "lluv.utils"
local FSM            = require "lluv.pg.utils.fsm"
local MessageEncoder = require "lluv.pg.msg".encoder
local MessageDecoder = require "lluv.pg.msg".decoder
local decode_type    = require "lluv.pg.msg".decode_type

local function on_server_error(fsm, event, ctx, data)
  local err = MessageDecoder.ErrorResponse(data)
  ctx:on_error(err)
  return ctx, err
end

local function on_protocol_error(fsm, event, ctx, data)
  local err = {
    name  = "EPROTO";
    msg   = "Protocol error";
    state = fsm:active();
    event = event;
    data  = data;
  }
  ctx:on_protocol_error(err)
  return ctx, err
end

local function on_status(fsm, event, ctx, data)
  local key, val = MessageDecoder.ParameterStatus(data)
  ctx:on_status(key, val)
  return ctx, key, val
end

local function on_notice(fsm, event, ctx, data)
  local res = MessageDecoder.NoticeResponse(data)
  ctx:on_notice(res)
  return ctx, res
end

local function on_notify(fsm, event, ctx, data)
  local pid, name, payload = MessageDecoder.NotificationResponse(data)
  ctx:on_notify(pid, name, payload)
  return ctx, pid, name, payload
end

local function on_ready(fsm, event, ctx, data)
  ctx:on_ready()
end

local function on_terminate(fsm, event, ctx, data)
  ctx:on_send(MessageEncoder.Terminate())
  ctx:on_terminate()
end

local Base = ut.class() do

function Base:reset()
  self._fsm:reset()
  return self
end

function Base:step(event, data)
  event, data = MessageDecoder.dispatch(event, data)
  return self._fsm:step(event, self, data)
end

--- FSM need send data.
-- 
function Base:on_send(header, msg) end

--- Server return error message (ErrorResponse)
--
function Base:on_error(err) end

--- Server send change status message (ParameterStatus)
--
function Base:on_status(key, value) end

---
--
function Base:on_notice(note) end

---
--
function Base:on_notify(pid, name, payload) end

--- Got unexpected message.
--
function Base:on_protocol_error() end

--- FSM done success (finit state)
--
function Base:on_ready() end

--- FSM fail (finit state)
-- Connection is in undefined state and should be closed
--
function Base:on_terminate() end

end

local Setup = ut.class(Base) do

local fsm = FSM.new("setup") 

fsm:action("server_error",    on_server_error)

fsm:action("protocol_error",  on_protocol_error)

fsm:action("decode_status",   on_status)

fsm:action("decode_notice",   on_notice)

fsm:action("decode_notify",   on_notify)

fsm:action("send_md5_auth",   function(self, event, ctx, data)
  local _, salt = MessageDecoder.AuthenticationMD5Password(data)

  local user, password = ctx:on_need_password()

  local digest = md5.digest(password .. user)
  digest = "md5" .. md5.digest(digest .. salt)

  ctx:on_send(MessageEncoder.PasswordMessage(digest))
end)

fsm:action("send_clear_auth", function(self, event, ctx, data)
  MessageDecoder.AuthenticationCleartextPassword(data)

  local user, password = ctx:on_need_password()
  ctx:on_send(MessageEncoder.PasswordMessage(password))
end)

fsm:action("decode_pidkey",   function(self, event, ctx, data)
  local pid, key = MessageDecoder.BackendKeyData(data)
  ctx:on_backend_key(pid, key)
end)

fsm:state("setup", {
  ['*']                 = {"protocol_error",  "terminate"};
  ErrorResponse         = {"server_error",    "terminate"};
  ParameterStatus       = {"decode_status"               };
  NoticeResponse        = {"decode_notice"               };
  BackendKeyData        = {"decode_pidkey"               };
  NotificationResponse  = {"decode_notify"               };
  ReadyForQuery         = {nil,               "ready"    };

  AuthenticationMD5Password        = {"send_md5_auth",    "wait_auth_response" };
  AuthenticationCleartextPassword  = {"send_clear_auth",  "wait_auth_response" };
})

fsm:state("wait_auth_response", {
  ['*']                 = {"protocol_error",  "terminate"};
  ErrorResponse         = {"server_error",    "terminate"};
  ParameterStatus       = {"decode_status"               };
  NoticeResponse        = {"decode_notice"               };
  NotificationResponse  = {"decode_notify"               };
  ReadyForQuery         = {nil,               "ready"    };
  AuthenticationOk      = {nil,               "auth_done"};
})

fsm:state("auth_done",{
  ['*']                 = {"protocol_error",  "terminate"};
  ErrorResponse         = {"server_error",    "terminate"};
  ParameterStatus       = {"decode_status"               };
  NoticeResponse        = {"decode_notice"               };
  BackendKeyData        = {"decode_pidkey"               };
  NotificationResponse  = {"decode_notify"               };
  ReadyForQuery         = {nil,               "ready"    };
})

fsm:state("ready", on_ready)

fsm:state("terminate", on_terminate)

function Setup:__init()
  self._fsm = fsm:clone():reset()
  return self
end

function Setup:start(ver, opt)
  self:on_send(MessageEncoder.greet(ver, opt))
  self._fsm:start()
end

function Setup:on_backend_key(pid, key) end

function Setup:on_need_password() end

end

local SimpleQuery = ut.class(Base) do

local fsm = FSM.new("wait_response") 

fsm:action("server_error",    on_server_error)

fsm:action("protocol_error",  on_protocol_error)

fsm:action("decode_status",   on_status)

fsm:action("decode_notify",   on_notify)

fsm:action("new_rs",          function(self, event, ctx, data)
  local rows, n = MessageDecoder.RowDescription(data)
  local cols, typs = {}, {}

  for i, desc in ipairs(rows) do
    local ltyp = decode_type(desc[2])
    cols[#cols + 1] = desc[1]
    typs[#typs + 1] = ltyp
  end

  ctx:on_new_rs{cols, typs}
end)

fsm:action("decode_row",      function(self, event, ctx, data)
  local row = MessageDecoder.DataRow(data)
  ctx:on_row(row)
end)

fsm:action("close_rs",        function(self, event, ctx, data)
  local cmd, rows = MessageDecoder.CommandComplete(data)
  ctx:on_close_rs(rows)
end)

fsm:action("exec_complite",   function(self, event, ctx, data)
  local cmd, rows = MessageDecoder.CommandComplete(data)
  ctx:on_exec(rows)
end)

fsm:action("empty",           function(self, event, ctx, data)
  ctx:on_close_rs(0)
end)

fsm:state("wait_response", {
  ['*']            = {"protocol_error",  "terminate"};
  ParameterStatus  = {"decode_status"               };
  NoticeResponse   = {"decode_notice"               };

  ErrorResponse    = {"server_error",  "error_recived"};
  ReadyForQuery    = {nil,             "ready"        };

  EmptyQueryResponse = {};
  CommandComplete    = {"exec_complite"};

  RowDescription   = {"new_rs",          "wait_row_data" };
})

fsm:state("wait_row_data",{
  ['*']            = {"protocol_error",  "terminate"     };
  ErrorResponse    = {"server_error",    "error_recived" };
  ParameterStatus  = {"decode_status"                    };
  ReadyForQuery    = {nil,               "ready"         };

  DataRow          = {"decode_row"                       };
  CommandComplete  = {"close_rs",        "wait_response" };
})

fsm:state("error_recived",{
  ['*']         = {};
  ReadyForQuery = { nil, "ready" };
})

fsm:state("ready", on_ready)

fsm:state("terminate", on_terminate)

function SimpleQuery:__init()
  self._fsm = fsm:clone():reset()
  return self
end

function SimpleQuery:start(qry)
  self:on_send(MessageEncoder.Query(qry))
  self._fsm:start()
end

function SimpleQuery:on_new_rs(desc) end

function SimpleQuery:on_row(desc) end

function SimpleQuery:on_close_rs(rows) end

function SimpleQuery:on_exec(rows) end

end

local Idle = ut.class(Base) do
local fsm = FSM.new("wait")

fsm:action("decode_status",  on_status)

fsm:action("decode_notify",  on_notify)

fsm:action("protocol_error", on_protocol_error)

fsm:state("wait", {
  ['*']                 = {"protocol_error",  "terminate"};
  ParameterStatus       = {"decode_status"               };
  NotificationResponse  = {"decode_notify"               };
})

fsm:state("terminate", on_terminate)

function Idle:__init()
  self._fsm = fsm:clone():reset()
  return self
end

function Idle:start()
  self._fsm:start()
end

end

local MessageReader = ut.class() do

function MessageReader:__init()
  self._buf = ut.Buffer.new()
  return self
end

local function next_msg(buf)
  local header = buf:read(5)
  if not header then return end

  local typ, len = struct.unpack(">c1I4", header)
  assert(len >= 4, string.format("%q", typ) .. "/" .. tostring(len))

  local data = buf:read(len - 4)
  if not data then
    buf:prepend(buf)
    return
  end

  return typ, data
end

function MessageReader:append(data)
  self._buf:append(data)
  while true do
    local typ, msg = next_msg(self._buf)
    if not typ then break end
    local ret = self:on_message(typ, msg)
    if not ret then break end
  end
end

function MessageReader:close()
  self._buf:reset()
  self._buf = nil
end

end

local FSMReader = ut.class(MessageReader) do

function FSMReader:__init(fsm)
  MessageReader.__init(self)
  self._fsm  = fsm
  self._done = false
  
  return self
end

function FSMReader:on_message(typ, data)
  if not self._fsm:step(typ, data) then
    self._done = true
    return false
  end
  return true
end

function FSMReader:done()
  return not not self._done
end

function FSMReader:reset(fsm)
  if fsm then self._fsm  = fsm end
  self._done = false
  return self
end

end

return{
  Setup         = Setup;
  SimpleQuery   = SimpleQuery;
  Idle          = Idle;

  MessageReader = MessageReader;
  FSMReader     = FSMReader;
}
