local prequire = function(m)
  local ok, m = pcall(require, m)
  if ok then return m end
end

local struct = require "lluv.pg.utils.bin"
local cjson  = prequire"cjson"

local function unpack_int(n)
  local fmt = '>i' .. n
  return function(data) return (struct.unpack(fmt, data)) end;
end

local function pack_int(n)
  local fmt = '>i' .. n
  return function(value) return struct.pack(fmt, value) end;
end

local function fail(msg)
  return function() error(msg, 3) end
end

local function pass(data)
  return data
end

local decode_int = function(n)
  return {
    [0] = tonumber;
    [1] = unpack_int(n)
  }
end

local encode_int = function(n)
  return {
    [0] = tostring;
    [1] = pack_int(n);
  }
end

local decode_float = function(n)
  assert( (n == 4) or (n == 8) )

  --[[
  local fmt
  if n == struct.size('f') then
    fmt = 'f'
  elseif n == struct.size('d') then
    fmt = 'f'
  end
  --]]

  local bin
  if not fmt then
    bin = fail(string.format('Unsupported binary mode for float%d type', n))
  else
    bin = function(data) return struct.unpack(fmt, data) end;
  end

  return {
    [0] = tonumber;
    [1] = bin;
  }
end

local encode_float = function(n)
  assert( (n == 4) or (n == 8) )

  --[[
  local fmt
  if n == struct.size('f') then
    fmt = 'f'
  elseif n == struct.size('d') then
    fmt = 'f'
  end
  --]]

  local bin
  if not fmt then
    bin = fail(string.format('Unsupported binary mode for float%d type', n))
  else
    bin = function(value) return struct.pack(fmt, value) end;
  end

  return {
    [0] = tostring;
    [1] = bin;
  }
end

local decode_bool = function()
  return {
    [0] = function(data) return data ~= 'f'  end;
    [1] = function(data) return data ~= '\0' end;
  }
end

local encode_bool = function()
  return {
    [0] = function(value) return value and 't'  or 'f'  end;
    [1] = function(value) return value and '\1' or '\0' end;
  }
end

local decode_bin = function()
  return {[0] = pass; [1] = pass;}
end

local encode_bin = function()
  return {[0] = pass; [1] = pass;}
end

local decode_json = function()
  return {[0] = cjson.decode; [1] = cjson.decode;}
end

local encode_json = function()
  return {[0] = cjson.encode; [1] = cjson.encode;}
end

local function decode_numeric_bin(data)
  local ndigits, weight, sign, dscale, pos = struct.unpack('>i2i2i2i2', data )

  local r = sign == 0 and '' or '-'

  for i = 1, ndigits do
    local d d, pos = struct.unpack('>i2', data, pos)
    r = r .. tostring(d)
  end

  if dscale > 0 then
    r = string.sub(r, 1, -dscale - 1) .. '.' .. string.sub(r, -dscale)
  end

  return r
end

local decode_numeric = function()
  return {
    [0] = pass;
    [1] = decode_numeric_bin;
  }
end

local encode_numeric = function(n)
  return {
    [0] = tostring;
    [1] = fail('Unsupported binary mode for numeric type');
  }
end

local decode_date = function(n)
  return {
    [0] = pass;
    [1] = n == 12 and 
      
    decode_int(n);
  }
end

local encode_date = function(n)
  return {
    [0] = tostring;
    [1] = fail('Unsupported binary mode for numeric type');
  }
end

local function int(n)
  return {
    encode = encode_int(n);
    decode = decode_int(n);
  }
end

local function float(n)
  return {
    encode = encode_float(n);
    decode = decode_float(n);
  }
end

local function bool()
  return {
    encode = encode_bool();
    decode = decode_bool();
  }
end

local function bin()
  return {
    encode = encode_bin();
    decode = decode_bin();
  }
end

local function json()
  if not cjson then return end

  return {
    encode = encode_json();
    decode = decode_json();
  }
end

local function numeric()
  return {
    encode = encode_numeric();
    decode = decode_numeric();
  }
end

local function date(n)
  return {
    encode = encode_date(n);
    decode = decode_date(n);
  }
end

local converters = {
  bool    = bool();

  int2    = int(2);
  int4    = int(4);
  int8    = int(8);
  regproc = int(4);
  oid     = int(4);
  tid     = int(4);
  xid     = int(4);
  cid     = int(4);
  json    = json();
  jsonb   = json();
  float4  = float(4);
  float8  = float(8);
  numeric = numeric();

  abstime     = date(4);
  date        = date(4);
  time        = date(8);
  timestamp   = date(8);
  timestamptz = date(8);
  time_stamp  = date(8);

  -- timetz      = date(12);
  -- interval    = date(16);
  -- tinterval   = date(12);

  text    = bin();
  bytea   = bin();
  char    = bin();
  name    = bin();
  xml     = bin();

--   point
--   lseg
--   path
--   box
--   polygon
--   line
--   cidr
--   abstime
--   reltime
--   tinterval
--   unknown
--   circle
--   money
--   macaddr
--   inet
--   aclitem
--   bpchar
--   varchar
--   bit
--   varbit
--   refcursor
--   regprocedure
--   regoper
--   regoperator
--   regclass
--   regtype
--   record
--   cstring
--   uuid
--   txid_snapshot
--   pg_lsn
--   tsvector
--   tsquery
--   gtsvector
--   regconfig
--   regdictionary
--   int4range
--   numrange
--   tsrange
--   tstzrange
--   daterange
--   int8range
--   regnamespace
--   regrole
};

do
  local array_converters = {}

  for k, v in pairs(converters) do
    if not k:find('array::') then
      array_converters['array::' .. k] = v
    end
  end

  for type_name, converter in pairs(array_converters) do
    converters[type_name] = converter;
  end
end

local function get_decoder(typ)
  local tname, mode = typ[1], typ[3]
  local converter = converters[tname]
  if not converter then return end
  return converter.decode and converter.decode[mode]
end

local function get_encoder(typ)
  local tname, mode = typ[1], typ[3]
  local converter = converters[tname]
  if not converter then return end
  return converter.encode and converter.encode[mode]
end

return {
  encoder      = get_encoder;
  decoder      = get_decoder;
}