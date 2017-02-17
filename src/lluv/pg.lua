local uv     = require "lluv"
local socket = require "lluv.luasocket"
local ut     = require "lluv.utils"

local Setup         = require "lluv.pg.fsm".Setup
local SimpleQuery   = require "lluv.pg.fsm".SimpleQuery
local MessageReader = require "lluv.pg.fsm".MessageReader
local Idle          = require "lluv.pg.fsm".Idle
local Prepare       = require "lluv.pg.fsm".Prepare
local Execute       = require "lluv.pg.fsm".Execute

local MessageEncoder = require "lluv.pg.msg".encoder

local function append(t, v) t[#t + 1] = v end

local function call_q(q, ...)
  while true do
    local cb = q:pop()
    if not cb then break end
    cb(...)
  end
end

local function is_callable(f)
  return (type(f) == 'function') and f
end

local EOF       = uv.error("LIBUV", uv.EOF)
local ENOTCONN  = uv.error('LIBUV', uv.ENOTCONN)
local ECANCELED = uv.error('LIBUV', uv.ECANCELED)

local Connection = ut.class() do

function Connection:__init(cfg)
  self._ready   = false
  self._status  = {}
  self._bkey    = {}
  self._reader  = MessageReader.new()
  self._open_q  = nil
  self._close_q = nil
  self._queue   = nil

  self._pg_opt = {
    database = assert(cfg.database);
    user     = cfg.user or "postgres";
  }

  self._cnn_opt = {
    host     = cfg.host or '127.0.0.1';
    port     = cfg.port or 5432;
    password = cfg.password;
  }

  local this = self

  function this._on_send(fsm, header, msg)            this:send(header, msg)              end

  function this._on_protocol_error(fsm, err)          this._last_error = err              end

  function this._on_error(fsm, err)                   this._last_error = err              end

  function this._on_status(fsm, key, value)           this._status[key] = value           end

  function this._on_backend_key(fsm, pid, key, value) this._bkey = {pid = pid, key = key} end

  function this._on_notice(fsm, note)                 this:on_notice(note)                end

  function this._on_notify(fsm, pid, name, payload)   this:on_notify(pid, name, payload)  end

  function self._reader:on_message(typ, msg)
    if not this._fsm then return true end

    if not this._fsm:step(typ, msg) then
      this._fsm = this._idle
      return false
    end

    return true
  end

  self._setup = Setup.new()
  self._setup.on_send           = this._on_send
  self._setup.on_error          = this._on_error
  self._setup.on_protocol_error = this._on_protocol_error
  self._setup.on_status         = this._on_status
  self._setup.on_backend_key    = this._on_backend_key
  self._setup.on_notice         = this._on_notice
  self._setup.on_notify         = this._on_notify
  self._setup.on_need_password  = function()
    return this._pg_opt.user, this._cnn_opt.password
  end
  self._setup.on_ready          = function()
    self._ready = true
    call_q(this._open_q, this, nil, true)
    uv.defer(this._next_query, this)
  end
  self._setup.on_terminate      = function()
    this:close(this._last_error)
  end

  self._idle = Idle.new()
  self._idle.on_send           = this._on_send
  self._idle.on_protocol_error = this._on_protocol_error
  self._idle.on_terminate      = function() this:close() end
  self._idle.on_status         = this._on_status
  self._idle.on_notice         = this._on_notice
  self._idle.on_notify         = this._on_notify

  self._query = SimpleQuery.new()
  self._query.on_send           = this._on_send
  self._query.on_error          = this._on_error
  self._query.on_protocol_error = this._on_protocol_error
  self._query.on_status         = this._on_status
  self._query.on_backend_key    = this._on_backend_key
  self._query.on_notice         = this._on_notice
  self._query.on_notify         = this._on_notify

  self._prepare = Prepare.new()
  self._prepare.on_send           = this._on_send
  self._prepare.on_error          = this._on_error
  self._prepare.on_protocol_error = this._on_protocol_error
  self._prepare.on_status         = this._on_status
  self._prepare.on_backend_key    = this._on_backend_key
  self._prepare.on_notice         = this._on_notice
  self._prepare.on_notify         = this._on_notify

  self._execute = Execute.new()
  self._execute.on_send           = this._on_send
  self._execute.on_error          = this._on_error
  self._execute.on_protocol_error = this._on_protocol_error
  self._execute.on_status         = this._on_status
  self._execute.on_backend_key    = this._on_backend_key
  self._execute.on_notice         = this._on_notice
  self._execute.on_notify         = this._on_notify

  return self
end

function Connection:connect(cb)
  if self._ready then
    if cb then uv.defer(cb, self, nil, self._cli) end
    return
  end

  if not self._open_q then
    self._open_q  = ut.Queue.new()
    self._close_q = ut.Queue.new()
    self._queue   = ut.Queue.new()
  end

  if cb then self._open_q:push(cb) end

  if self._cli then return end

  local host, port = self._cnn_opt.host, self._cnn_opt.port

  self._cli = uv.tcp():connect(host, port, function(cli, err)
    if err then return self:close(err) end

    self:_start_read()

    self._fsm = self._setup:reset()
    self._last_error = nil

    self._fsm:start("3.0", self._pg_opt)
  end)
end

function Connection:cancel(cb)
  uv.tcp():connect("127.0.0.1", 5432, function(cli, err)
    if err then
      cli:close()
      if cb then cb(self, err) end
      return
    end
    local h, m = MessageEncoder.CancelRequest(self._bkey.pid, self._bkey.key)
    cli:write(h .. m, function(cli, err)
      cli:close()
      if cb then cb(self, err) end
    end)
  end)
end

function Connection:_start_read()
  self._cli:start_read(function(cli, err, data)
    if err then return self:close(err) end

    self._reader:append(data)
  end)
end

function Connection:query(...)
  if not self._cli then
    local cb = is_callable(select(-1, ...))
    return uv.defer(cb, self, ENOTCONN)
  end

  self._queue:push{...}

  return self:_next_query()
end

function Connection:_next_query()
  -- connection already execute query so we have to wait
  if self._fsm ~= self._idle then
    return
  end

  self._last_error = nil

  local args = self._queue:pop()
  if not args then return end

  local sql, params, cb = args[1], args[2], args[3]
  if is_callable(params) then
    cb, params = params
    return self:_next_simple_query(sql, cb)
  end

  return self:_next_extended_query(sql, params, cb)
end

function Connection:_next_simple_query(sql, cb)
  self._fsm = self._query:reset()

  local this, rs = self, {}

  function self._fsm:on_new_rs(desc) append(rs, {header = desc}) end

  function self._fsm:on_row(row) append(rs[#rs], row) end

  function self._fsm:on_close_rs(rows) end

  function self._fsm:on_exec(rows) append(rs, {rows}) end

  function self._fsm:on_ready()
    uv.defer(cb, this, this._last_error, rs)
    uv.defer(this._next_query, this)
  end

  function self._fsm:on_terminate()
    uv.defer(cb, this, this._last_error, rs)
    this:close(this._last_error)
  end

  self._fsm:start(sql)
end

function Connection:_next_extended_query(sql, params, cb)
  local statement_name = ''
  self:_prepare_query(statement_name, sql, function(self, err, rs, formats)
    if err then
      uv.defer(self._next_query, self)
      return uv.defer(cb, self, err)
    end

    return self:_execute_query(statement_name, params, function(self, err, rows)
      uv.defer(self._next_query, self)
      cb(self, err, rows)
    end)
  end)
end

function Connection:_prepare_query(name, sql, cb)
  self._fsm = self._prepare:reset()

  local this, rs, params = self, {}

  function self._fsm:on_new_rs(desc) rs = desc end

  function self._fsm:on_params(params_types) params = params_types end

  function self._fsm:on_ready()
    uv.defer(cb, this, this._last_error, rs, params)
  end

  function self._fsm:on_terminate()
    uv.defer(cb, this, this._last_error, rs)
    this:close(this._last_error)
  end

  self._fsm:start(name, sql)
end

function Connection:_execute_query(name, params, cb)
  local portal = ''

  self._fsm = self._execute:reset()

  local this, rs = self, {}

  function self._fsm:on_new_rs(desc) rs.header = desc end

  function self._fsm:on_row(row) append(rs, row) end

  function self._fsm:on_close_rs(rows) end

  function self._fsm:on_suspended() rs.suspended = true end

  function self._fsm:on_exec(rows) append(rs, rows) end

  function self._fsm:on_ready()
    uv.defer(this._next_query, self)
    uv.defer(cb, this, this._last_error, rs, params)
  end

  function self._fsm:on_terminate()
    uv.defer(cb, this, this._last_error, rs)
    this:close(this._last_error)
  end

  self._fsm:start(portal, name, nil, params, 0)
end

function Connection:close(err, cb)
  if type(err) == 'function' then
    cb, err = err
  end

  if not self._cli then
    if cb then uv.defer(cb, self) end
    return
  end

  if cb then self._close_q:push(cb) end

  if not (self._cli:closed() or self._cli:closing()) then
    self._cli:close(function()
      local open_q, close_q, q  = self._open_q, self._close_q, self._queue

      self._cli, self._open_q, self._close_q, self._queue = nil

      call_q(open_q, self, err or EOF)

      while true do
        local args = q:pop()
        if not args then break end
        local cb = is_callable(args[3]) or is_callable(args[2])
        cb(self, err)
      end

      call_q(close_q, self, err)
    end)
  end

  self._ready = false
end

function Connection:connected()
  return not not self._ready
end

function Connection:send(header, data)
  -- print("SEND EVENT:", header, data)
  return self._cli:write{header, data}
end

end

return {
  new = Connection.new
}
