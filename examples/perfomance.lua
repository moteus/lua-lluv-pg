
local CONFIG = {
  database = os.getenv'LLUV_PG_DBNAME' or 'test',
  user     = os.getenv'LLUV_PG_DBUSER' or 'postgres',
  password = os.getenv'LLUV_PG_DBPASS' or '',
}

local function prequire(m)
  local ok, mod = pcall(require, m)
  if ok then return mod end
  return nil, mod
end

local timer_start, timer_stop do

local zmq  = prequire "lzmq"

if zmq then

function timer_start()
  local timer = zmq.utils.stopwatch():start()
  return timer
end

function timer_stop(timer)
  local elapsed = timer:stop()
  return elapsed, 1000000
end

else

function timer_start()
  return os.clock()
end

function timer_stop(timer)
  return os.clock() - timer, 1
end

end

end

local function odbc_perform(query_count, sql)
  local odbc = prequire "odbc"
  if not odbc then return print("odbc not installed") end

  local database = odbc.assert(odbc.connect{
    driver = 'PostgreSQL ANSI'; Server='127.0.0.1'; Port='5432';
    Database = CONFIG.database;
    Uid      = CONFIG.user;
    Pwd      = CONFIG.password;
  })

  local stmt = database :statement()
  local timer = timer_start()

  for i = 1, query_count do
    stmt:execute(sql):fetch()
    stmt:close()
  end

  local elapsed, resolution = timer_stop(timer)
  local throughput = query_count / (elapsed / resolution)
  print(string.format("odbc mean throughput: %.2f [qry/s]", throughput))

  stmt:destroy()
  database:destroy()
end

local function moon_perform_impl(name, database, query_count, sql)
  assert(database:connect())

  local timer = timer_start()

  for i = 1, query_count do
    database:query(sql)
  end

  local elapsed, resolution = timer_stop(timer)
  local throughput = query_count / (elapsed / resolution)
  print(string.format("%s mean throughput: %.2f [qry/s]", name, throughput))

  database:disconnect()
end

local function moon_perform(...)
  local moon = prequire "pgmoon"
  if not moon then return print("pgmoon not installed") end

  local database = moon.new{
    host = "127.0.0.1", port = "5432",
    database = CONFIG.database;
    user     = CONFIG.user;
    password = CONFIG.password;
  }

  return moon_perform_impl('moon', database, ...)
end

local function lluv_perform(database_count, query_count, sql)
  local pg   = require "lluv.pg"
  local uv   = require "lluv"

  database_count = database_count or 1
  local databases = {}
  for i = 1, database_count do
    databases[i] = pg.new{
      database = CONFIG.database;
      user     = CONFIG.user;
      password = CONFIG.password;
    }
  end

  local counter, timer = query_count
  local function execute(self, err, res)
    counter = counter - 1

    if counter == 0 then
      local elapsed, resolution = timer_stop(timer)
      local throughput = query_count / (elapsed / resolution)
      print(string.format("lluv(%d) mean throughput: %.2f [qry/s]", database_count, throughput))
      execute = nil
      return uv.stop()
    end

    self:query(sql, execute)
  end

  local n = database_count
  for i = 1, database_count do
    databases[i]:connect(function()
      n = n - 1
      if n == 0 then
        timer = timer_start()
        for i = 1, database_count do
          execute(databases[i])
        end
      end
    end)
  end

  uv.run()
  for i = 1, database_count do
    databases[i]:close()
  end
  uv.run()
end

local function moon_lluv_perform(query_count, sql)
  local moon = prequire "pgmoon"
  if not moon then return print("pgmoon not installed") end
  if not prequire'pgmoon.lluv' then return print("pgmoon-lluv not installed") end

  local uv   = require "lluv"
  local ut   = require "lluv.utils"

  ut.corun(function()
    local database = moon.new{
      host = "127.0.0.1", port = "5432",
      database = CONFIG.database;
      user     = CONFIG.user;
      password = CONFIG.password;
      socket_type = 'lluv';
    }

    return moon_perform_impl('moon-lluv', database, query_count, sql)
  end)

  uv.run()
end

local function colluv_perform(query_count, sql)
  local uv   = require "lluv"
  local ut   = require "lluv.utils"
  local pg   = require "lluv.pg.co"

  ut.corun(function()
    local database = pg.new{
      database = CONFIG.database;
      user     = CONFIG.user;
      password = CONFIG.password;
    }
    database:connect()

    local timer = timer_start()

    for i = 1, query_count do
      database:query(sql)
    end

    local elapsed, resolution = timer_stop(timer)
    local throughput = query_count / (elapsed / resolution)
    print(string.format("co-lluv mean throughput: %.2f [qry/s]", throughput))

    database:close()
  end)

  uv.run()
end

-- we really need simple query because we need compare only
-- protocol overhead.
local query_count = 50000
local sql = "select 'hello'::text"

odbc_perform(query_count, sql)
moon_perform(query_count, sql)
moon_lluv_perform(query_count, sql)
lluv_perform(1, query_count, sql)
lluv_perform(2, query_count, sql)
lluv_perform(3, query_count, sql)
lluv_perform(4, query_count, sql)
colluv_perform(query_count, sql)
