package = "lluv-pg"
version = "scm-0"

source = {
  url = "https://github.com/moteus/lua-lluv-pg/archive/master.zip",
  dir = "lua-lluv-pg-master",
}

description = {
  summary    = "PostgreSQL client for lluv library",
  homepage   = "https://github.com/moteus/lua-lluv-pg",
  license    = "MIT/X11",
  maintainer = "Alexey Melnichuk",
  detailed   = [[
  ]],
}

dependencies = {
  "lua >= 5.1, < 5.4",
  "lluv > 0.1.1",
  "eventemitter",
  "struct >= 1.2",
}

build = {
  copy_directories = {'test'},

  type = "builtin",

  modules = {
    ["lluv.odbc"        ] = "src/lua/lluv/odbc.lua",
    ["lluv.odbc.common" ] = "src/lua/lluv/odbc/common.lua",
    ["lluv.odbc.thread" ] = "src/lua/lluv/odbc/thread.lua",
  }
}
