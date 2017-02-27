local struct_unpack, struct_pack

if string.pack then -- Lua 5.3
  struct_unpack, struct_pack, struct_size = string.unpack, string.pack
elseif not jit then -- Lua 5.1, 5.3
  local struct = require "struct"
  struct_unpack, struct_pack, struct_size = struct.unpack, struct.pack
else -- LuaJIT

local unpack = unpack or table.unpack

local bit = require "bit"

local function sign1(n)
  if n >= 0x80 then
    n = -1 - bit.band(0xFF, bit.bnot(n) )
  end
  return n
end

local function sign2(n)
  if n >= 0x8000 then
    n = -1 - bit.band(0xFFFF, bit.bnot(n) )
  end
  return n
end

local function sign4(n)
  if n >= 0x80000000 then
    n = -1 - bit.band(0xFFFFFFFF, bit.bnot(n) )
  end
  return n
end

local function read_byte(str, pos)
  pos = pos or 1
  local a = string.sub(str, pos, pos)
  if a then
    return string.byte(a), pos + 1
  end
  return nil, pos
end

local function read_sbyte(str, pos)
  local b
  b, pos = read_byte(str, pos)
  return sign1(b), pos
end

local function read_2_bytes(str, pos)
  local a
  a, pos = read_byte(str, pos)
  return a, read_byte(str, pos)
end

local function read_4_bytes(str, pos)
  local a, b
  a, b, pos = read_byte(str, pos)
  return a, b, read_byte(str, pos)
end

local function read_be_uint2(str, pos)
  local a, b
  a, b, pos = read_2_bytes(str, pos)
  return bit.lshift(a, 8) + b, pos
end

local function read_be_int2(str, pos)
  local n 
  n, pos = read_be_uint2(str, pos)

  return sign2(n), pos
end

local function read_le_uint2(str, pos)
  local a, b
  a, b, pos = read_2_bytes(str, pos)
  return a + bit.lshift(b,8), pos
end

local function read_le_int2(str, pos)
  local n 
  n, pos = read_le_uint2(str, pos)

  return sign2(n), pos
end

local function read_be_uint4(str, pos)
  local a, b
  a, pos = read_be_uint2(str, pos)
  b, pos = read_be_uint2(str, pos)
  return bit.lshift(a, 16) + b, pos
end

local function read_be_int4(str, pos)
  local n
  n, pos = read_be_uint4(str, pos)

  return sign4(n), pos
end

local function read_le_uint4(str, pos)
  local a, b
  a, pos = read_le_uint2(str, pos)
  b, pos = read_le_uint2(str, pos)
  return a + bit.lshift(b, 16), pos
end

local function read_le_int4(str, pos)
  local n
  n, pos = read_le_uint4(str, pos)

  return sign4(n), pos
end

local function read_zstr(str, pos)
  local e = string.find(str, '\0', pos, true)
  if e then
    return string.sub(str, pos or 1, e-1), e + 1
  end
  return nil, pos
end

local function read_chars(str, n, pos)
  pos = pos or 1
  local s = string.sub(str, pos, pos + n - 1)
  return s, pos + #s
end

local function pack_byte(n)
  return string.char((bit.band(0xFF, n)))
end

local function pack_sbyte(n)
  if n < 0 then
    n = bit.bnot(-n) + 1
  end

  return pack_byte(n)
end

local function pack_le_uint2(n)
  n = bit.band(0xFFFF, n)
  return string.char(bit.band(n, 0x00FF), bit.rshift(n, 8))
end

local function pack_le_int2(n)
  if n < 0 then 
    n = bit.bnot(-n) + 1
  end
  return pack_le_uint2(n)
end

local function pack_be_uint2(n)
  n = bit.band(0xFFFF, n)
  return string.char(bit.rshift(n, 8), bit.band(n, 0x00FF))
end

local function pack_be_int2(n)
  if n < 0 then 
    n = bit.bnot(-n) + 1
  end
  return pack_be_uint2(n)
end

local function pack_le_uint4(n)
  n = bit.band(0xFFFFFFFF, n)
  return string.char(
    bit.band(0xFF, bit.rshift(n, 0 )),
    bit.band(0xFF, bit.rshift(n, 8 )),
    bit.band(0xFF, bit.rshift(n, 16)),
    bit.band(0xFF, bit.rshift(n, 24))
  )
end

local function pack_be_uint4(n)
  n = bit.band(0xFFFFFFFF, n)
  return string.char(
    bit.band(0xFF, bit.rshift(n, 24)),
    bit.band(0xFF, bit.rshift(n, 16)),
    bit.band(0xFF, bit.rshift(n, 8 )),
    bit.band(0xFF, bit.rshift(n, 0 ))
  )
end

local function pack_be_int4(n)
  if n < 0 then 
    n = bit.bnot(-n) + 1
  end
  return pack_be_uint4(n)
end

local function pack_le_int4(n)
  if n < 0 then 
    n = bit.bnot(-n) + 1
  end
  return pack_le_uint4(n)
end

local function pack_zstr(s)
  return s .. '\0'
end

local function printf(...) print(string.format(...)) end

local sunpack do

local unpack_int = {
  ['<'] = {
    I = {
      ['4'] = read_le_uint4;
      ['2'] = read_le_uint2;
    };
    i = {
      ['4'] = read_le_int4;
      ['2'] = read_le_int2;
    };
  };
  ['>'] = {
    I = {
      ['4'] = read_be_uint4;
      ['2'] = read_be_uint2;
    };
    i = {
      ['4'] = read_be_int4;
      ['2'] = read_be_int2;
    };
  };
}

unpack_int['!'] = unpack_int['<']

local unpack_str = {
  c = read_chars;
  s = function(str, _, pos) return read_zstr (str, pos) end;
  b = function(str, _, pos) return read_sbyte(str, pos) end;
  B = function(str, _, pos) return read_byte (str, pos) end;
}

local res = {}

sunpack = function(fmt, str, pos)
  local i, endian = 0, '>'

  for e, p, n in string.gmatch(fmt, "([<>!]?)([iIscbB])(%d?)") do
    if e ~= '' then endian = e end

    if p ~= '' then
      i = i + 1
      local fn = unpack_str[p]
      if fn then
        res[i], pos = fn(str, n, pos)
      elseif unpack_int[endian][p] then
        fn = unpack_int[endian][p][n]
        res[i], pos = fn(str, pos)
      else
        error('unsupported format: ' .. p)
      end
    end
  end

  i = i + 1; res[i] = pos
  return unpack(res, 1, i)
end

end

local spack do

local pack_int = {
  ['<'] = {
    I = {
      ['4'] = pack_le_uint4;
      ['2'] = pack_le_uint2;
    };
    i = {
      ['4'] = pack_le_int4;
      ['2'] = pack_le_int2;
    };
  };
  ['>'] = {
    I = {
      ['4'] = pack_be_uint4;
      ['2'] = pack_be_uint2;
    };
    i = {
      ['4'] = pack_be_int4;
      ['2'] = pack_be_int2;
    };
  };
}

pack_int['!'] = pack_int['<']

local pack_str = {
  c = function(str, n) return string.sub(str, 1, n) end;
  s = pack_zstr;
  b = pack_sbyte;
  B = pack_byte;
}

local res = {}

spack = function(fmt, ...)
  local i, endian = 0, '>'

  for e, p, n in string.gmatch(fmt, "([<>!]?)([iIscbB])(%d?)") do
    if e ~= '' then endian = e end
    if p ~= '' then
      i = i + 1
      local v = select(i, ...)

      local fn = pack_str[p]
      if fn then
        res[i], pos = fn(v, tonumber(n))
      elseif pack_int[endian][p] then
        fn = pack_int[endian][p][n]
        res[i], pos = fn(v)
      else
        error('unsupported format: ' .. p)
      end
    end
  end

  return table.concat(res, '', 1, i)
end

end

struct_pack, struct_unpack = spack, sunpack

end

return {
  pack   = struct_pack;
  unpack = struct_unpack;
}
