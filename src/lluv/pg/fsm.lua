local md5            = require "md5"
local ut             = require "lluv.utils"
local struct         = require "lluv.pg.utils.bin"
local FSM            = require "lluv.pg.utils.fsm"
local MessageEncoder = require "lluv.pg.msg".encoder
local MessageDecoder = require "lluv.pg.msg".decoder
local DataTypes      = require "lluv.pg.types"
local utils          = require "lluv.pg.utils"
local Error          = require "lluv.pg.error"

local append, super = utils.append, utils.super

local PGServerError, PGProtoError = Error.ServerError, Error.ProtoError

local function on_server_error(fsm, event, ctx, data)
  local err = MessageDecoder.ErrorResponse(data)
  err = PGServerError.new(err)
  ctx.on_error(ctx._self, err)
  return ctx, err
end

local function on_protocol_error(fsm, event, ctx, data)
  local err = PGProtoError.new(fsm:active(), event, data)
  ctx.on_protocol_error(ctx._self, err)
  return ctx, err
end

local function on_status(fsm, event, ctx, data)
  local key, val = MessageDecoder.ParameterStatus(data)
  ctx.on_status(ctx._self, key, val)
  return ctx, key, val
end

local function on_notice(fsm, event, ctx, data)
  local res = MessageDecoder.NoticeResponse(data)
  ctx.on_notice(ctx._self, res)
  return ctx, res
end

local function on_notify(fsm, event, ctx, data)
  local pid, name, payload = MessageDecoder.NotificationResponse(data)
  ctx.on_notify(ctx._self, pid, name, payload)
  return ctx, pid, name, payload
end

local function on_ready(fsm, event, ctx, data)
  local status = MessageDecoder.ReadyForQuery(data)
  ctx.on_ready(ctx._self, status)
end

local function on_terminate(fsm, event, ctx, data)
  ctx.on_send(ctx._self, MessageEncoder.Terminate())
  ctx.on_terminate(ctx._self)
end

local function on_new_recordset(self, event, ctx, data)
  local rows, n = MessageDecoder.RowDescription(data)
  local cols, typs = {}, {}

  for i, desc in ipairs(rows) do
    cols[#cols + 1] = desc[1]
    desc[1] = DataTypes.type_name(desc[2])
    typs[#typs + 1] = desc
  end

  ctx.on_new_rs(ctx._self, {cols, typs})
end

local function on_data_row(self, event, ctx, data)
  local row = MessageDecoder.DataRow(data)
  ctx.on_row(ctx._self, row)
end

local function on_close_recordset(self, event, ctx, data)
  local cmd, rows = MessageDecoder.CommandComplete(data)
  ctx.on_close_rs(ctx._self, rows)
end

-- calls if execute empty query
local function on_empty_recordset(self, event, ctx, data)
  ctx.on_empty_rs(ctx._self)
end

local function on_portal_suspended(self, event, ctx, data)
  MessageDecoder.PortalSuspended(data)
  ctx.on_suspended(ctx._self)
end

local function on_execute(self, event, ctx, data)
  local cmd, rows = MessageDecoder.CommandComplete(data)
  ctx.on_exec(ctx._self, rows)
end

local Base = ut.class() do

function Base:__init(opt)
  self._self = opt and opt.self or self

  return self
end

function Base:reset()
  self._fsm:reset()
  return self
end

function Base:step(event, data)
  event, data = MessageDecoder.dispatch(event, data)
  -- print("RECV EVENT:", event)
  return self._fsm:step(event, self, data)
end

function Base:send(header, msg)
  self.on_send(self._self, header, msg)
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
function Base:on_notify(pid, channel, payload) end

--- Got unexpected message.
--
function Base:on_protocol_error(err) end

--- FSM done success (finit state)
--
function Base:on_ready() end

--- FSM fail (finit state)
-- Connection is in undefined state and should be closed
--
function Base:on_terminate() end

end

local function InitFSM(...)
  local fsm = FSM.new(...)

  -- ACTIONS

  fsm:action("server_error",    on_server_error)

  fsm:action("protocol_error",  on_protocol_error)

  fsm:action("decode_status",   on_status)

  fsm:action("decode_notify",   on_notify)

  fsm:action("decode_notice",   on_notice)

  fsm:action("new_rs",          on_new_recordset)

  fsm:action("decode_row",      on_data_row)

  fsm:action("close_rs",        on_close_recordset)

  fsm:action("empty_rs",        on_empty_recordset)

  fsm:action("exec_complite",   on_execute)

  fsm:action("suspended",       on_portal_suspended)

  -- STATES

  fsm:state("ready",            on_ready)

  fsm:state("terminate",        on_terminate)

  return fsm
end

local function InitState(a, b)
  local err_state, t = a, b
  if not t then err_state, t = nil, a end

  local default = {
    ['*']                 = {"protocol_error", "terminate"     };
    ParameterStatus       = {"decode_status"                   };
    NoticeResponse        = {"decode_notice"                   };
    NotificationResponse  = {"decode_notify"                   };
  }

  if err_state then
    default.ErrorResponse = {"server_error",   err_state       };
  end

  for k, v in pairs(default) do
    if not t[k] then t[k] = v end
  end
  return t
end

local S = InitState

local Setup = ut.class(Base) do

local fsm = InitFSM("setup")

fsm:action("send_md5_auth",   function(self, event, ctx, data)
  local _, salt = MessageDecoder.AuthenticationMD5Password(data)

  local user, password = ctx.on_need_password(ctx._self)

  local digest = md5.digest(password .. user)
  digest = "md5" .. md5.digest(digest .. salt)

  ctx:send(MessageEncoder.PasswordMessage(digest))
end)

fsm:action("send_clear_auth", function(self, event, ctx, data)
  MessageDecoder.AuthenticationCleartextPassword(data)

  local user, password = ctx.on_need_password(ctx._self)
  ctx:send(MessageEncoder.PasswordMessage(password))
end)

fsm:action("decode_pidkey",   function(self, event, ctx, data)
  local pid, key = MessageDecoder.BackendKeyData(data)
  ctx.on_backend_key(ctx._self, pid, key)
end)

fsm:state("setup", S("terminate", {
  BackendKeyData        = {"decode_pidkey"               };
  ReadyForQuery         = {nil,               "ready"    };

  AuthenticationOk                 = {nil,                "auth_done"          };
  AuthenticationMD5Password        = {"send_md5_auth",    "wait_auth_response" };
  AuthenticationCleartextPassword  = {"send_clear_auth",  "wait_auth_response" };
}))

fsm:state("wait_auth_response", S("terminate", {
  ReadyForQuery         = {nil,               "ready"    };
  AuthenticationOk      = {nil,               "auth_done"};
}))

fsm:state("auth_done", S("terminate", {
  BackendKeyData        = {"decode_pidkey"               };
  ReadyForQuery         = {nil,               "ready"    };
}))

function Setup:__init(...)
  self = super(Setup, self, '__init', ...)

  self._fsm = fsm:clone():reset()
  return self
end

function Setup:start(ver, opt)
  self:send(MessageEncoder.greet(ver, opt))
  self._fsm:start()
end

function Setup:on_backend_key(pid, key) end

function Setup:on_need_password() end

end

local SimpleQuery = ut.class(Base) do

local fsm = InitFSM("wait_response")

fsm:state("wait_response", S("error_recived", {
  ReadyForQuery      = {nil,            "ready"         };
  EmptyQueryResponse = {"empty_rs"                      };
  CommandComplete    = {"exec_complite"                 };
  RowDescription     = {"new_rs",       "wait_row_data" };
}))

fsm:state("wait_row_data",S("error_recived", {
  ReadyForQuery    = {nil,              "ready"         };
  DataRow          = {"decode_row"                      };
  CommandComplete  = {"close_rs",       "wait_response" };
}))

fsm:state("error_recived", S("error_recived", {
  ['*']            = {};
  ReadyForQuery    = {nil,              "ready"         };
}))

function SimpleQuery:__init(...)
  self = super(SimpleQuery, self, '__init', ...)

  self._fsm = fsm:clone():reset()
  return self
end

function SimpleQuery:start(qry)
  self:send(MessageEncoder.Query(qry))
  self._fsm:start()
end

function SimpleQuery:on_new_rs(desc) end

function SimpleQuery:on_row(desc) end

function SimpleQuery:on_close_rs(rows) end

function SimpleQuery:on_empty_rs() end

function SimpleQuery:on_exec(rows) end

end

local Idle = ut.class(Base) do

local fsm = InitFSM("wait")

fsm:state("wait", S{})

function Idle:__init(...)
  self = super(Idle, self, '__init', ...)

  self._fsm = fsm:clone():reset()
  return self
end

function Idle:start()
  self._fsm:start()
end

end

local Prepare = ut.class(Base) do

local fsm = InitFSM("wait")

fsm:action("decode_params",  function(self, event, ctx, data)
  local typs = MessageDecoder.ParameterDescription(data)
  ctx.on_params(ctx._self, typs)
end)

fsm:state("wait", S("describe", {
  ParseComplete         = {nil,               "describe"      };
}))

fsm:state("describe", S("describe", {
  ParameterDescription  = {"decode_params"                    };
  RowDescription        = {"new_rs"                           };
  NoData                = {};
  ReadyForQuery         = {nil,               "ready"         };
}))

function Prepare:__init(...)
  self = super(Prepare, self, '__init', ...)

  self._fsm = fsm:clone():reset()
  return self
end

function Prepare:start(name, sql, opt)
  self:send(MessageEncoder.Parse(name, sql, opt))
  self:send(MessageEncoder.Describe("S", name))
  self:send(MessageEncoder.Sync())

  self._fsm:start()
end

function Prepare:on_new_rs() end

function Prepare:on_params() end

end

local Execute = ut.class(Base) do

local fsm = InitFSM("binding")

fsm:state("binding", S("closing", {
  BindComplete          = {nil,               "describing"    };
}))

fsm:state("describing", S("closing", {
  -- Describe portal response
  RowDescription     = {"new_rs",             "fetching"      };
  NoData             = {};

  -- Execute responses
  CommandComplete    = {"exec_complite",      "closing"       };
  EmptyQueryResponse = {"empty_rs",           "closing"       };

  -- Execute responses if we do not send Describe
  DataRow            = {"decode_row",         "fetching"      };
  PortalSuspended    = {"suspended",          "closing"       };
}))

fsm:state("fetching", S("closing", {
  DataRow            = {"decode_row"                          };
  PortalSuspended    = {"suspended",          "closing"       };
  CommandComplete    = {"close_rs",           "closing"       };
}))

fsm:state("closing", S("wait_ready", {
  ReadyForQuery         = {"send_close", "wait_ready" };
}))

fsm:state("wait_ready", S("wait_ready", {
  CloseComplete         = {};
  ReadyForQuery         = {nil,     "ready" };
}))

fsm:action("send_close", function(self, event, ctx, data)
  if ctx._portal ~= '' then
    ctx:send(MessageEncoder.Close("P", ctx._portal))
  end
  -- Send sync to get `ReadyForQuery`
  ctx:send(MessageEncoder.Sync())
end)

function Execute:__init(...)
  self = super(Execute, self, '__init', ...)

  self._fsm = fsm:clone():reset()
  return self
end

function Execute:start(describe, portal, name, formats, values, rows)
  self._portal = portal

  self._fsm:start()

  -- we have to send commands with sync
  -- in other case tehre may be no response from server.
  -- In my test without sync I did not get `BindComplete`

  self:send(
    MessageEncoder.Bind(portal, name, formats, values)
  )

  if describe then
    self:send(
      MessageEncoder.Describe("P", portal)
    )
  end

  self:send(
    MessageEncoder.Execute(portal, rows)
  )

  self:send(MessageEncoder.Sync())

end

function Execute:on_suspended() end

function Execute:on_empty_rs() end

end

local Close = ut.class(Base) do

local fsm = InitFSM("wait")

fsm:state("wait", S{
  CloseComplete         = {};
  ReadyForQuery         = {nil,     "ready" };
})

function Close:__init(...)
  self = super(Close, self, '__init', ...)

  self._fsm = fsm:clone():reset()
  return self
end

-- `P` portal
-- `S` prepared statement
function Close:start(typ, name)
  assert(typ == 'S' or typ == 'P')

  self._fsm:start()
  self:send(MessageEncoder.Close(typ, name))
  -- Send sync to get `ReadyForQuery`
  self:send(MessageEncoder.Sync())
end

end

local MessageReader = ut.class() do

function MessageReader:__init(opt)
  self._buf  = ut.Buffer.new()
  self._typ  = nil
  self._len  = nil
  self._self = opt and opt.self or self

  return self
end

local function next_msg(self)
  if not self._typ then
    local header = self._buf:read(5)
    if not header then return end

    self._typ, self._len = struct.unpack(">c1I4", header)
    assert(self._len >= 4, string.format("%q", self._typ) .. "/" .. tostring(self._len))
  end

  local data, typ = self._buf:read(self._len - 4), self._typ
  if not data then return end

  self._typ, self._len = nil

  return typ, data
end

function MessageReader:append(data)
  self._buf:append(data)
  while true do
    local typ, msg = next_msg(self)
    if not typ then break end
    local ret = self.on_message(self._self, typ, msg)
    if not ret then break end
  end
end

function MessageReader:close()
  self._buf:reset()
  self._buf, self._typ, self._len = nil
end

end

local FSMReader = ut.class(MessageReader) do

function FSMReader:__init(fsm)
  self = super(FSMReader, self, '__init')
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
  Prepare       = Prepare;
  Execute       = Execute;
  Close         = Close;

  MessageReader = MessageReader;
  FSMReader     = FSMReader;
}
