local uv             = require "lluv"
local socket         = require "lluv.luasocket"
local ut             = require "lluv.utils"
local EventEmitter   = require "EventEmitter"
local Setup          = require "lluv.pg.fsm".Setup
local SimpleQuery    = require "lluv.pg.fsm".SimpleQuery
local MessageReader  = require "lluv.pg.fsm".MessageReader
local Idle           = require "lluv.pg.fsm".Idle
local Prepare        = require "lluv.pg.fsm".Prepare
local Execute        = require "lluv.pg.fsm".Execute
local NULL           = require "lluv.pg.msg".NULL
local DataTypes      = require "lluv.pg.types"
local Array          = require "lluv.pg.array"
local Converter      = require "lluv.pg.converter"
local MessageEncoder = require "lluv.pg.msg".encoder
local utils          = require "lluv.pg.utils"

local append, call_q, is_callable =
  utils.append, utils.call_q, utils.is_callable

local EOF       = uv.error("LIBUV", uv.EOF)
local ENOTCONN  = uv.error('LIBUV', uv.ENOTCONN)
local ECANCELED = uv.error('LIBUV', uv.ECANCELED)

local Connection = ut.class() do

local function translate_desc(self, resultset, desc)
  local types = desc[2]

  if self._settings.decode then
    for i = 1, #types do
      local type_desc = types[i]
      type_desc.decode = Converter.decoder(type_desc)
    end
  end

  resultset.header = desc

  return resultset
end

local function translate_data_row(self, resultset, row)
  local types = resultset.header[2]

  for i = 1, #types do
    local type_desc = types[i]
    if DataTypes.is_array(type_desc) then
      row[i] = Array.decode(type_desc[3], row[i], type_desc.decode)
    elseif type_desc.decode then
      row[i] = type_desc.decode(row[i])
    end
  end

  append(resultset, row)
end

function Connection:__init(cfg)
  self._ready   = false
  self._ee      = EventEmitter.new{self=self}
  self._status  = {}
  self._bkey    = {}
  self._open_q  = nil
  self._close_q = nil
  self._queue   = nil
  self._active  = {
    resultset = nil;
    callback  = nil;
    params    = nil;
  }

  self._pg_opt = {
    database = assert(cfg.database);
    user     = cfg.user or "postgres";
  }

  if cfg.config then
    for k, v in pairs(cfg.config) do
      self._pg_opt[k] = v
    end
  end

  self._cnn_opt = {
    host     = cfg.host or '127.0.0.1';
    port     = cfg.port or 5432;
    password = cfg.password;
  }

  self._settings = {
    decode = cfg.decode; -- decode data
  }

  self._reader = MessageReader.new{self=self}
  self._reader.on_message       = self._on_message

  self._setup = Setup.new{self=self}
  self._setup.on_send           = self._on_send
  self._setup.on_error          = self._on_error
  self._setup.on_protocol_error = self._on_protocol_error
  self._setup.on_terminate      = self._on_terminate
  self._setup.on_status         = self._on_status
  self._setup.on_backend_key    = self._on_backend_key
  self._setup.on_notice         = self._on_notice
  self._setup.on_notify         = self._on_notify
  self._setup.on_need_password  = self._on_setup_need_password
  self._setup.on_ready          = self._on_setup_ready

  self._idle = Idle.new{self=self}
  self._idle.on_send           = self._on_send
  self._idle.on_protocol_error = self._on_protocol_error
  self._idle.on_terminate      = self._on_terminate
  self._idle.on_status         = self._on_status
  self._idle.on_notice         = self._on_notice
  self._idle.on_notify         = self._on_notify

  self._query = SimpleQuery.new{self=self}
  self._query.on_send           = self._on_send
  self._query.on_error          = self._on_error
  self._query.on_protocol_error = self._on_protocol_error
  self._query.on_terminate      = self._on_terminate
  self._query.on_status         = self._on_status
  self._query.on_backend_key    = self._on_backend_key
  self._query.on_notice         = self._on_notice
  self._query.on_notify         = self._on_notify
  self._query.on_exec           = self._on_query_exec
  self._query.on_empty_rs       = self._on_query_empty_rs
  self._query.on_new_rs         = self._on_query_new_rs
  self._query.on_row            = self._on_query_row
  self._query.on_close_rs       = self._on_query_close_rs
  self._query.on_ready          = self._on_query_ready

  self._prepare = Prepare.new{self=self}
  self._prepare.on_send           = self._on_send
  self._prepare.on_error          = self._on_error
  self._prepare.on_protocol_error = self._on_protocol_error
  self._prepare.on_terminate      = self._on_terminate
  self._prepare.on_status         = self._on_status
  self._prepare.on_backend_key    = self._on_backend_key
  self._prepare.on_notice         = self._on_notice
  self._prepare.on_notify         = self._on_notify
  self._prepare.on_new_rs         = self._on_prepare_new_rs
  self._prepare.on_params         = self._on_prepare_params
  self._prepare.on_ready          = self._on_prepare_ready

  self._execute = Execute.new{self=self}
  self._execute.on_send           = self._on_send
  self._execute.on_error          = self._on_error
  self._execute.on_protocol_error = self._on_protocol_error
  self._execute.on_terminate      = self._on_terminate
  self._execute.on_status         = self._on_status
  self._execute.on_backend_key    = self._on_backend_key
  self._execute.on_notice         = self._on_notice
  self._execute.on_notify         = self._on_notify
  self._execute.on_exec           = self._on_execute_exec
  self._execute.on_new_rs         = self._on_execute_new_rs
  self._execute.on_row            = self._on_execute_row
  self._execute.on_close_rs       = self._on_execute_close_rs
  self._execute.on_empty_rs       = self._on_execute_empty_rs
  self._execute.on_suspended      = self._on_execute_suspended
  self._execute.on_ready          = self._on_execute_ready

  return self
end

do -- FSM Events

function Connection:_on_send(header, msg)
  self:send(header, msg)
end

function Connection:_on_protocol_error(err)
  self._last_error = err
  self._ee:emit('error', err)
end

function Connection:_on_error(err)
  self._last_error = err
  self._ee:emit('error', err)
end

function Connection:_on_status(key, value)
  self._status[key] = value
  self._ee:emit('status', key, value)
end

function Connection:_on_backend_key(pid, key, value)
  self._bkey = {pid = pid, key = key}
end

function Connection:_on_notice(note)
  self._ee:emit('notice', note)
end

function Connection:_on_notify(pid, name, payload)
  self._ee:emit('notify', pid, name, payload)
end

function Connection:_on_terminate()
  local callback = self._active.callback
  if callback then
    self:_reset_active_state()
    uv.defer(callback, self, self._last_error, rs)
  end
  uv.defer(self.close, self, self._last_error)
end

function Connection:_on_message(typ, msg)
  self._ee:emit('recv', typ, msg)

  if not self._fsm then return true end

  if not self._fsm:step(typ, msg) then
    self._fsm = self._idle
    return false
  end

  return true
end

function Connection:_on_write_error(err)
  if err then 
    if err ~= EOF then
      self._ee:emit('error', err)
    end
    self:close(err)
  end
end

do -- Setup

function Connection:_on_setup_need_password()
  if not self._cnn_opt.password then error('no password') end
  return self._pg_opt.user, self._cnn_opt.password
end

function Connection:_on_setup_ready()
  self._ready = true
  self._ee:emit('ready', self._status)
  call_q(self._open_q, self, nil, true)
  uv.defer(self._next_query, self)
end

end

do -- Simple Query

function Connection:_on_query_exec(rows)
  local resultset = self._active.resultset
  append(resultset, {rows})
end

function Connection:_on_query_empty_rs()
  local resultset = self._active.resultset
  append(resultset, {})
end

function Connection:_on_query_new_rs(desc)
  local resultset = self._active.resultset
  append(resultset, translate_desc(self, {}, desc))
end

function Connection:_on_query_row(row)
  local resultset = self._active.resultset
  resultset = resultset[#resultset]
  translate_data_row(self, resultset, row)
end

function Connection:_on_query_close_rs(rows) end

function Connection:_on_query_ready()
  local callback  = self._active.callback
  local resultset = self._active.resultset

  self:_reset_active_state()

  local count = #resultset
  if count <= 1 then resultset = resultset[1] end

  uv.defer(callback, self, self._last_error, resultset, count)
  uv.defer(self._next_query, self)
end

end

do -- Prepare

function Connection:_on_prepare_new_rs(desc)
  self._active.resultset = translate_desc(self, {}, desc)
end

function Connection:_on_prepare_params(params)
  self._active.params = params
end

function Connection:_on_prepare_ready()
  local params    = self._active.params
  local callback  = self._active.callback
  local resultset = self._active.resultset

  self:_reset_active_state()

  uv.defer(callback, self, self._last_error, resultset, params)
end

end

do -- Execute

function Connection:_on_execute_exec(rows)
  local resultset = self._active.resultset
  if not resultset.header then
    append(resultset, rows)
  end
end

function Connection:_on_execute_new_rs(desc)
  local resultset = self._active.resultset
  translate_desc(self, resultset, desc)
end

function Connection:_on_execute_row(row)
  local resultset = self._active.resultset
  translate_data_row(self, resultset, row)
end

function Connection:_on_execute_close_rs(rows) end

function Connection:_on_execute_empty_rs() end

function Connection:_on_execute_suspended()
  local resultset = self._active.resultset
  resultset.suspended = true
end

function Connection:_on_execute_ready()
  local callback  = self._active.callback
  local resultset = self._active.resultset

  self:_reset_active_state()

  uv.defer(callback, self, self._last_error, resultset, 1)
  uv.defer(self._next_query, self)
end

end

end

function Connection:connect(cb)
  if self._ready then
    if cb then uv.defer(cb, self) end
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
    if err then
      self._ee:emit('error', err)
      return self:close(err)
    end

    self._ee:emit('open')

    self:_start_read()

    self._fsm = self._setup:reset()
    self._last_error = nil

    self._fsm:start("3.0", self._pg_opt)
  end)
end

function Connection:cancel(cb)
  if not self._ready then
    if cb then uv.defer(cb, self, ENOTCONN) end
    return
  end

  local host, port = self._cnn_opt.host, self._cnn_opt.port

  uv.tcp():connect(host, port, function(cli, err)
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

function Connection:_reset_active_state()
  self._active.params, self._active.resultset, self._active.callback = nil
end

function Connection:_start_read()
  self._cli:start_read(function(cli, err, data)
    if err then
      if err ~= EOF then
        self._ee:emit('error', err)
      end
      return self:close(err)
    end

    self._reader:append(data)
  end)
end

function Connection:query(...)
  if not self._cli then
    local cb = is_callable(select(-1, ...))
    return uv.defer(cb, self, ENOTCONN)
  end

  self._queue:push{'query', ...}

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

  local action = args[1]

  if action == 'query' then
    sql, params, cb = args[2], args[3], args[4]
    if is_callable(params) then
      cb, params = params
      return self:_next_simple_query(sql, cb)
    end
    return self:_next_extended_query(sql, params, cb)
  end
end

function Connection:_next_simple_query(sql, cb)
  self._fsm = self._query:reset()

  self._active.resultset = {}
  self._active.callback  = cb
  self._active.params    = nil

  self._fsm:start(sql)
end

function Connection:_next_extended_query(sql, params, cb)
  local statement_name = ''
  self:_prepare_query(statement_name, sql, function(self, err, rs, formats)
    if err then
      uv.defer(self._next_query, self)
      return uv.defer(cb, self, err)
    end

    return self:_execute_query(statement_name, params, rs, cb)
  end)
end

function Connection:_prepare_query(name, sql, cb)
  self._fsm = self._prepare:reset()

  self._active.callback  = cb
  self._active.resultset = nil
  self._active.params    = nil

  self._fsm:start(name, sql)
end

function Connection:_execute_query(name, params, resultset, cb)
  if is_callable(resultset) then
    cb, resultset = resultset
  end

  local portal = ''

  self._fsm = self._execute:reset()

  self._active.callback  = cb
  self._active.resultset = resultset or {}
  self._active.params    = nil

  self._fsm:start(not resultset, portal, name, nil, params, 0)
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

      local active_callback = self._active.callback

      self._cli, self._open_q, self._close_q, self._queue = nil
      self:_reset_active_state()

      if active_callback then active_callback(self, err or EOF) end

      call_q(open_q, self, err or EOF)

      while true do
        local args = q:pop()
        if not args then break end
        local cb = is_callable(args[#args])
        cb(self, err or EOF)
      end

      call_q(close_q, self, err)

      self._ee:emit('close')
    end)
  end

  self._ready = false
end

function Connection:connected()
  return not not self._ready
end

local function on_write_done(cli, err, self)
  if err then self:_on_write_error(err) end
end

function Connection:send(header, data)
  self._ee:emit('send', header, data)
  return self._cli:write({header, data}, on_write_done, self)
end

function Connection:on(...)
  return self._ee:on(...)
end

function Connection:off(...)
  return self._ee:off(...)
end

function Connection:onAny(...)
  return self._ee:onAny(...)
end

function Connection:offAny(...)
  return self._ee:offAny(...)
end

function Connection:removeAllListeners(...)
  return self._ee:removeAllListeners(...)
end

end

return {
  new  = Connection.new;
  NULL = NULL;
}
