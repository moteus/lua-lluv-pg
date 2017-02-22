local lpeg   = require "lpeg"
local struct = require "struct"
local NULL   = require "lluv.pg.msg".NULL

local P, S, V, Ct, C, Cs = lpeg.P, lpeg.S, lpeg.V, lpeg.Ct, lpeg.C, lpeg.Cs

local literal = function(name)
  if name == 'NULL' then return NULL end
  return name
end

local g = P{
  "array",

  array = Ct(V("open") * (V("value") * (P(",") * V("value")) ^ 0) ^ -1 * V("close")),

  value = V("invalid_char") + V("string") + V("array") + V("literal"),

  string = P('"') * Cs((P([[\\]]) / [[\]] + P([[\"]]) / [["]] + (P(1) - P('"'))) ^ 0) * P('"'),

  literal = C((P(1) - S("},")) ^ 1) / literal,

  invalid_char = S(" \t\r\n") / function()
    return error("got unexpected whitespace")
  end,

  open   = P("{"),
  delim  = P(","),
  close  = P("}"),
}

local function decode_text_array(data)
  return g:match(data)
end

local decode_binary_array do

local function read_data(data, pos)
  local len len, pos = struct.unpack(">i4", data, pos)
  if len < 0 then return NULL, pos end
  if len == 0 then return '', pos end
  return string.sub(data, pos, pos + len - 1), pos + len
end

local function read_dim(res, no, str, pos, offset)
  local dim, idx

  dim, offset = struct.unpack('>I4', str, offset)
  idx, offset = struct.unpack('>I4', str, offset)

  if no > 1 then
    for i = 1, dim do
      res[i + idx - 1], pos = read_dim({}, no - 1, str, pos, offset)
    end
    return res, pos
  end

  for i = 1, dim do
    res[i + idx - 1], pos = read_data(str, pos)
  end

  return res, pos
end

decode_binary_array = function(data, pos)
  -- array reperesnt as
  -- {dim}{offset}{oid}{{dim,idx},...}{{len,data},...}

  local dim -- # of dimensions

  dim, pos = struct.unpack('>I4', data, pos)
  pos = pos + 4 -- offset to data, or 0 if no bitmap (seems unused to decode)
  pos = pos + 4 -- element type OID

  local offset = pos  -- pos where start dims metadata
  pos = pos + dim * 8 -- skip dims metadata

  local res = read_dim({}, dim, data, pos, offset)

  return res
end

end

local function decode_array(mode, data, pos)
  if data == NULL then return data end

  if mode == 0 then
    return decode_text_array(data)
  end

  return decode_binary_array(data, pos)
end

return {
  decode = decode_array;
}