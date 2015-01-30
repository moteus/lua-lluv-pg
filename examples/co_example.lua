local uv     = require "lluv"
local ut     = require "lluv.utils"
local pg     = require "lluv.pg.co"

ut.corun(function()
  local cnn = pg.new{
    database = 'mydb',
    password = '***',
  }

  cnn:connect()

  local sql = "select cast('hello' as text) as f1, cast(5.2 as float4) as f2, cast(5.2 as float8) as f3"

  print(cnn:query(sql))

  sql = "update pg_am set amname=amname"

  print(cnn:query(sql))

  sql = "select cast('hello' as text) as f1;" ..
        "update pg_am set amname=amname"

  print(cnn:query(sql))

  cnn:disconnect()
end)

uv.run()
