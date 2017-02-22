
--! @fixme sync this arrays with pg_type.h

local arrays = {
  [ 143  ] = "array::xml";
  [ 199  ] = "array::json";
  [ 629  ] = "array::line";
  [ 651  ] = "array::cidr";
  [ 719  ] = "array::circle";
  [ 791  ] = "array::money";
  [ 1000 ] = "array::bool";
  [ 1001 ] = "array::bytea";
  [ 1002 ] = "array::char";
  [ 1003 ] = "array::name";
  [ 1005 ] = "array::int2";
  [ 1006 ] = "array::int2vector";
  [ 1007 ] = "array::int4";
  [ 1008 ] = "array::regproc";
  [ 1009 ] = "array::text";
  [ 1010 ] = "array::tid";
  [ 1011 ] = "array::xid";
  [ 1012 ] = "array::cid";
  [ 1013 ] = "array::oidvector";
  [ 1014 ] = "array::bpchar";
  [ 1015 ] = "array::varchar";
  [ 1016 ] = "array::int8";
  [ 1017 ] = "array::point";
  [ 1018 ] = "array::lseg";
  [ 1019 ] = "array::path";
  [ 1020 ] = "array::box";
  [ 1021 ] = "array::float4";
  [ 1022 ] = "array::float8";
  [ 1023 ] = "array::abstime";
  [ 1024 ] = "array::reltime";
  [ 1025 ] = "array::tinterval";
  [ 1027 ] = "array::polygon";
  [ 1028 ] = "array::oid";
  [ 1034 ] = "array::aclitem";
  [ 1040 ] = "array::macaddr";
  [ 1041 ] = "array::inet";
  [ 1115 ] = "array::timestamp";
  [ 1182 ] = "array::date";
  [ 1183 ] = "array::time";
  [ 1185 ] = "array::timestamptz";
  [ 1187 ] = "array::interval";
  [ 1231 ] = "array::numeric";
  [ 1263 ] = "array::cstring";
  [ 1270 ] = "array::timetz";
  [ 1561 ] = "array::bit";
  [ 1563 ] = "array::varbit";
  [ 2201 ] = "array::refcursor";
  [ 2207 ] = "array::regprocedure";
  [ 2208 ] = "array::regoper";
  [ 2209 ] = "array::regoperator";
  [ 2210 ] = "array::regclass";
  [ 2211 ] = "array::regtype";
  [ 2949 ] = "array::txid_snapshot";
  [ 2951 ] = "array::uuid";
  [ 3221 ] = "array::pg_lsn";
  [ 3643 ] = "array::tsvector";
  [ 3644 ] = "array::gtsvector";
  [ 3645 ] = "array::tsquery";
  [ 3735 ] = "array::regconfig";
  [ 3770 ] = "array::regdictionary";
  [ 3807 ] = "array::jsonb";
  [ 3905 ] = "array::int4range";
  [ 3907 ] = "array::numrange";
  [ 3909 ] = "array::tsrange";
  [ 3911 ] = "array::tstzrange";
  [ 3913 ] = "array::daterange";
  [ 3927 ] = "array::int8range";
  [ 4090 ] = "array::regnamespace";
  [ 4097 ] = "array::regrole";
}

local base = {
  [ 16   ] = "bool";
  [ 17   ] = "bytea";
  [ 18   ] = "char";
  [ 19   ] = "name";
  [ 20   ] = "int8";
  [ 21   ] = "int2";
  [ 23   ] = "int4";
  [ 24   ] = "regproc";
  [ 25   ] = "text";
  [ 26   ] = "oid";
  [ 27   ] = "tid";
  [ 28   ] = "xid";
  [ 29   ] = "cid";
  [ 114  ] = "json";
  [ 142  ] = "xml";
  [ 600  ] = "point";
  [ 601  ] = "lseg";
  [ 602  ] = "path";
  [ 603  ] = "box";
  [ 604  ] = "polygon";
  [ 628  ] = "line";
  [ 650  ] = "cidr";
  [ 700  ] = "float4";
  [ 701  ] = "float8";
  [ 702  ] = "abstime";
  [ 703  ] = "reltime";
  [ 704  ] = "tinterval";
  [ 705  ] = "unknown";
  [ 718  ] = "circle";
  [ 790  ] = "money";
  [ 829  ] = "macaddr";
  [ 869  ] = "inet";
  [ 1033 ] = "aclitem";
  [ 1042 ] = "bpchar";
  [ 1043 ] = "varchar";
  [ 1082 ] = "date";
  [ 1083 ] = "time";
  [ 1114 ] = "timestamp";
  [ 1184 ] = "timestamptz";
  [ 1186 ] = "interval";
  [ 1266 ] = "timetz";
  [ 1560 ] = "bit";
  [ 1562 ] = "varbit";
  [ 1700 ] = "numeric";
  [ 1790 ] = "refcursor";
  [ 2202 ] = "regprocedure";
  [ 2203 ] = "regoper";
  [ 2204 ] = "regoperator";
  [ 2205 ] = "regclass";
  [ 2206 ] = "regtype";
  [ 2249 ] = "record";
  [ 2275 ] = "cstring";
  [ 2950 ] = "uuid";
  [ 2970 ] = "txid_snapshot";
  [ 3220 ] = "pg_lsn";
  [ 3614 ] = "tsvector";
  [ 3615 ] = "tsquery";
  [ 3642 ] = "gtsvector";
  [ 3734 ] = "regconfig";
  [ 3769 ] = "regdictionary";
  [ 3802 ] = "jsonb";
  [ 3904 ] = "int4range";
  [ 3906 ] = "numrange";
  [ 3908 ] = "tsrange";
  [ 3910 ] = "tstzrange";
  [ 3912 ] = "daterange";
  [ 3926 ] = "int8range";
  [ 4089 ] = "regnamespace";
  [ 4096 ] = "regrole";
}

local function type_name(typ)
  return arrays[typ] or base[typ] or tostring(typ)
end

local function is_array(desc)
  return not not string.find(desc[1], 'array::', nil, true)
end

return {
  type_name = type_name;
  is_array  = is_array;
}