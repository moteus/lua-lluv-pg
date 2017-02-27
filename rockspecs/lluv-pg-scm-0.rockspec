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
  -- "struct >= 1.2",
  "lmd5",
  "null",
  "luuid",
}

build = {
  copy_directories = {'examples', 'spec'},

  type = "builtin",

  modules = {
    [ 'lluv.pg'           ] = 'src/lluv/pg.lua',
    [ 'lluv.pg.array'     ] = 'src/lluv/pg/array.lua',
    [ 'lluv.pg.co'        ] = 'src/lluv/pg/co.lua',
    [ 'lluv.pg.converter' ] = 'src/lluv/pg/converter.lua',
    [ 'lluv.pg.error'     ] = 'src/lluv/pg/error.lua',
    [ 'lluv.pg.fsm'       ] = 'src/lluv/pg/fsm.lua',
    [ 'lluv.pg.msg'       ] = 'src/lluv/pg/msg.lua',
    [ 'lluv.pg.types'     ] = 'src/lluv/pg/types.lua',
    [ 'lluv.pg.utils'     ] = 'src/lluv/pg/utils.lua',
    [ 'lluv.pg.utils.bin' ] = 'src/lluv/pg/utils/bin.lua',
    [ 'lluv.pg.utils.fsm' ] = 'src/lluv/pg/utils/fsm.lua',
  };
}
