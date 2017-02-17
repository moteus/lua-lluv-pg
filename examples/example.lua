local uv = require "lluv"
local ut = require "lluv.utils"
local pg = require "lluv.pg"

local cnn = pg.new{
  database = 'mydb',
  password = '***',
}

local cnn = pg.new{
  database = 'mydb',
  user     = 'postgres',
  password = 'secret',
}

cnn:connect(function(self, err)
  if err then
    return print('Connection error:', err)
  end
  print('Connection done')
end)

local function dump_query(self, err, res)
  if err then
    return print("Execute error:", err)
  end

  -- prepared query returns only one resultset
  if res.header then res = {res} end

  for _, h in ipairs(res[1].header[1]) do
    io.write(h, '\t')
  end

  io.write('\n', "====", '\n')
  for _, row in ipairs(res[1]) do
    for _, field in ipairs(row) do
      io.write(field, '\t')
    end
    io.write('\n')
  end
  io.write("----", '\n')
end

-- Simple query
local sql = "select 'hello'::text as f1, 5.2::float4 as f2, 5.2::float8 as f3"
cnn:query(sql, dump_query)

-- Query with parameters
local sql = "select $1::text as f1, $2::text as f2"
cnn:query(sql, {'hello', 'world'}, dump_query)

uv.run()
