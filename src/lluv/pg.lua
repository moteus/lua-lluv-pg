local uv     = require "lluv"
local socket = require "lluv.luasocket"
local ut     = require "lluv.utils"

local Setup         = require "lluv.pg.fsm".Setup
local SimpleQuery   = require "lluv.pg.fsm".SimpleQuery
local MessageReader = require "lluv.pg.fsm".MessageReader
local Idle          = require "lluv.pg.fsm".Idle

local MessageEncoder = require "lluv.pg.msg".encoder

local function append(t, v) t[#t + 1] = v end

local Connection = ut.class() do

function Connection:__init(cfg)
  self._status = {}
  self._bkey   = {}
  self._reader = MessageReader.new()

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

  return self
end

function Connection:connect(cb)
  if self._cli then
    return uv.defer(cb, self, "Not connected")
  end

  local host, port = self._cnn_opt.host, self._cnn_opt.port

  self._cli = uv.tcp():connect(host, port, function(cli, err)
    if err then
      cli:close()
      self._cli = nil
      return cb(self, err)
    end

    self:_start_read()

    self._fsm = self._setup:reset()
    self._last_error = nil
    local this = self

    function self._fsm:on_ready() uv.defer(cb, this) end

    function self._fsm:on_terminate()
      uv.defer(cb, this, this._last_error)
      self:close()
    end

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
    if err then
      self:close()
      if err:name() ~= 'EOF' then
        self._last_error = err
      end
      return
    end

    self._reader:append(data)
  end)
end

function Connection:query(sql, cb)
  if not self._cli then
    return uv.defer(self, self._last_error)
  end

  self._fsm = self._query:reset()

  self._last_error = nil

  local this, rs = self, {}

  function self._fsm:on_new_rs(desc) append(rs, {header = desc}) end

  function self._fsm:on_row(row) append(rs[#rs], row) end

  function self._fsm:on_close_rs(rows) end

  function self._fsm:on_exec(rows) append(rs, rows) end

  function self._fsm:on_ready() uv.defer(cb, this, this._last_error, rs) end

  function self._fsm:on_terminate()
    uv.defer(cb, this, this._last_error, rs)
    self:close()
  end

  self._fsm:start(sql)
end

function Connection:close()
  if self._cli    then self._cli:close()    end
  if self._reader then self._reader:close() end
  self._reader, self._cli = nil
end

function Connection:connected()
  return not not self._cli
end

function Connection:send(header, data)
  return self._cli:write{header, data}
end

end

return {
  new = Connection.new
}
