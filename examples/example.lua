local uv = require "lluv"
local ut = require "lluv.utils"
local pg = require "lluv.pg"

local cnn = pg.new{
  database = 'mydb',
  password = '***',
}

cnn:connect(function(self, err)
  if err then
    return print(err)
  end

  local sql = "select cast('hello' as text) as f1, cast(5.2 as float4) as f2, cast(5.2 as float8) as f3"

  self:query(sql, function(self, err, res)
    if err then print(err) end
    print(res)
    self:close()
  end)

end)

uv.run()
