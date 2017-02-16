local uv     = require "lluv"
local socket = require "lluv.luasocket"
local ut     = require "lluv.utils"
local FSM    = require "lluv.pg.fsm"

local Setup         = FSM.Setup
local SimpleQuery   = FSM.SimpleQuery
local Idle          = FSM.Idle
local FSMReader     = FSM.FSMReader
local Prepare       = FSM.Prepare
local Execute       = FSM.Execute

local function append(t, v) t[#t + 1] = v end

local function NewPG(cfg)
  local opt = {
    database = assert(cfg.database);
    user     = cfg.user or "postgres";
  }

  local cnn, srv_err
  local bkey, status = {}, {}
  local rs, rows_affected, col
  local host = cfg.host or '127.0.0.1'
  local port = cfg.port or 5432
  local txn  = 'I'
  local terminated = false

  local setup = Setup.new() do

  function setup:on_send(header, msg) cnn:send(header .. msg) end

  function setup:on_error(err) srv_err = err end

  function setup:on_protocol_error(err) srv_err = err end

  function setup:on_need_password() return opt.user, cfg.password end

  function setup:on_status(key, value) status[key] = value end

  function setup:on_backend_key(pid, key) bkey.pid, bkey.key = pid, key end

  function setup:on_ready(status) txn = status end

  function setup:on_terminate()
    terminated = true
    cnn:close()
  end

  end

  local query = SimpleQuery.new() do

  query.on_send           = setup.on_send
  query.on_error          = setup.on_error
  query.on_protocol_error = setup.on_protocol_error
  query.on_status         = setup.on_status
  query.on_ready          = setup.on_ready
  query.on_terminate      = setup.on_terminate

  function query:on_new_rs(desc)
    append(rs, { partial = true })
    col = desc[1]
  end

  function query:on_row(row)
    local t = {}
    for i, name in ipairs(col) do t[name] = row[i] end
    append(rs[#rs], t)
  end

  function query:on_close_rs(rows) rs[#rs].partial = nil end

  function query:on_exec(rows) append(rs, {affected_rows = rows}) end

  end

  local prepare = Prepare.new() do
  prepare.on_send           = query.on_send
  prepare.on_error          = query.on_error
  prepare.on_protocol_error = query.on_protocol_error
  prepare.on_status         = query.on_status
  prepare.on_ready          = query.on_ready
  prepare.on_terminate      = query.on_terminate
  prepare.on_new_rs         = query.on_new_rs
  end

  local execute = Execute.new() do
  execute.on_send           = query.on_send
  execute.on_error          = query.on_error
  execute.on_protocol_error = query.on_protocol_error
  execute.on_status         = query.on_status
  execute.on_ready          = query.on_ready
  execute.on_terminate      = query.on_terminate
  execute.on_new_rs         = query.on_new_rs
  execute.on_row            = query.on_row
  execute.on_close_rs       = query.on_close_rs
  execute.on_exec           = query.on_exec
  end

  local reader = FSMReader.new()

  local cli = {}

  function cli:disconnect()
    --! @todo send Terminate
    cnn:close()
    cnn = nil
  end

  function cli:connect()
    cnn, srv_err = socket.connect(host, port)
    if not cnn then return nil, srv_err end

    reader:reset(setup:reset())
    setup:start("3.0", opt)

    while not reader:done() do
      local data = assert(cnn:receive"*r")
      reader:append(data)
    end

    if srv_err then return nil, srv_err end

    return self
  end

  function cli:query(sql)
    rs, srv_err = {}

    reader:reset(query:reset())
    query:start(sql)

    while not reader:done() do
      local data = assert(cnn:receive"*r")
      reader:append(data)
    end

    local n = #rs
    if n == 1 then rs = rs[1] end
    if srv_err then return nil, srv_err, rs, n end
    return rs, n
  end

  function cli:execute(sql, values)
    rs, srv_err = {}

    reader:reset(prepare:reset())
    prepare:start('', sql, nil)

    while not reader:done() do
      local data = assert(cnn:receive"*r")
      reader:append(data)
    end

    if srv_err then return nil, srv_err end

    reader:reset(execute:reset())
    execute:start('', nil, values)

    while not reader:done() do
      local data = assert(cnn:receive"*r")
      reader:append(data)
    end

    rs = rs[1]
    if srv_err then return nil, srv_err, rs, n end

    rs.partial = nil
    return rs, 1
  end

  return cli
end

return {
  new = NewPG
}