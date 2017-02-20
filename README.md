# lua-lluv-pg
[![Licence](http://img.shields.io/badge/Licence-MIT-brightgreen.svg)](LICENSE)
[![Build Status](https://travis-ci.org/moteus/lua-lluv-pg.svg?branch=master)](https://travis-ci.org/moteus/lua-lluv-pg)
[![Coverage Status](https://coveralls.io/repos/github/moteus/lua-lluv-pg/badge.svg?branch=master)](https://coveralls.io/github/moteus/lua-lluv-pg?branch=master)

## PostgreSQL client based on lluv library
----

### Usage
```Lua
local cnn = pg.new{
  database = 'mydb',
  user     = 'postgres',
  password = 'secret',
  config = {
    application_name = 'lua-lluv-pg'
  },
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
```