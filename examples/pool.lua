package.path = "..\\src\\?.lua;" .. package.path

local uv = require "lluv"
local ut = require "lluv.utils"
local pg = require "lluv.pg"
local EventEmitter = require "EventEmitter".EventEmitter

local ENOTCONN  = uv.error('LIBUV', uv.ENOTCONN)

-- Pool does not track any query activity
-- it just do round robin for multiple connection
local PGConnectionPool = ut.class(EventEmitter) do
local Pool = PGConnectionPool

local function append(t, v)
  t[#t + 1] = v
  return t
end

local function remove_value(t, v)
  for i = 1, #t do
    if t[i] == v then
      return table.remove(t, i)
    end
  end
end

local function shift_value(t)
  local db = table.remove(t, 1)
  append(t, db)
  return db
end

local function super(self, m, ...)
  if self.__base and self.__base[m] then
    return self.__base[m](self, ...)
  end
  return self
end

function Pool:__init(n, cfg)
  self = super(self, '__init')

  self._active, self._waiting = {}, {}

  for i = 1, n do
    local db = pg.new(cfg)

    db:on('ready', function(db)
      if self._waiting[db] then
        self._waiting[db] = nil
        append(self._active, db)
      end
      self:emit('connection::ready', db)
    end)

    db:on('close', function(db, event, err)
      remove_value(self._active, db)
      self._waiting[db] = true
      self:emit('connection::close', db, err)
    end)

    append(self._active, db)

    db:connect()
  end

  return self
end

function Pool:query(...)
  local db = shift_value(self._active)
  if not db then
    local cb = select(-1, ...)
    if type(cb) == 'function' then
      return uv.defer(cb, db, ENOTCONN)
    end
    return
  end

  return db:query(...)
end

function Pool:close()
  for db in pairs(self._waiting) do
    db:close()
  end

  for _, db in ipairs(self._active) do
    db:close()
  end

  self._active  = {}
  self._waiting = {}
end

end

local pool = PGConnectionPool.new(5, {
  database  = 'mydb',
  user      = 'postgres',
  password  = 'secret',
  reconnect = 1;
})

uv.timer():start(0, 1000, function()
  pool:query('select $1::text', {'hello'}, function(self, err, rs)
    print(self, err or rs[1][1])
  end)
end):unref()

uv.timer():start(20000, function()
  pool:close()
end):unref()

pool:on('connection::ready', function(pool, _, db)
  print(db, 'connected')
end)

pool:on('connection::close', function(pool, _, db, err)
  print(db, 'disconnected', err)
end)

uv.run()
