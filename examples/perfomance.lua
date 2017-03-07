
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
  return zmq.utils.stopwatch():start()
end

function timer_stop(timer)
  return timer:stop(), 1000000
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

local function moon_perform(query_count, sql)
  local moon = prequire "pgmoon"
  if not moon then return print("pgmoon not installed") end

  local database = moon.new{
    host = "127.0.0.1", port = "5432",
    database = CONFIG.database;
    user     = CONFIG.user;
    password = CONFIG.password;
  }

  assert(database:connect())

  local timer = timer_start()

  for i = 1, query_count do
    database:query(sql)
  end

  local elapsed, resolution = timer_stop(timer)
  local throughput = query_count / (elapsed / resolution)
  print(string.format("moon mean throughput: %.2f [qry/s]", throughput))

  database:disconnect()
end

local function lluv_perform(query_count, sql)
  local pg   = require "lluv.pg"
  local uv   = require "lluv"

  local database = pg.new{
    database = CONFIG.database;
    user     = CONFIG.user;
    password = CONFIG.password;
  }

  local counter, timer = query_count
  local function execute(self, err, res)
    counter = counter - 1

    if counter == 0 then
      local elapsed, resolution = timer_stop(timer)
      local throughput = query_count / (elapsed / resolution)
      print(string.format("lluv mean throughput: %.2f [qry/s]", throughput))
      return uv.stop()
    end

    database:query(sql, execute)
  end

  database:connect(function()
    timer = timer_start()
    execute()
  end)

  uv.run()
  database:close()
  uv.run()
end

-- we really need simple query because we need compare only
-- protocol overhead.
local query_count = 10000
local sql = "select 'hello'::text"

odbc_perform(query_count, sql)
moon_perform(query_count, sql)
lluv_perform(query_count, sql)