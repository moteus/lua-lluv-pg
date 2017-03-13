local ut     = require "lluv.utils"
local append = require "lluv.pg.utils".append

local ERROR_PG = "PostgreSQL" -- error category

local PGServerError = ut.class() do

local CLASS = {
  ['00'] = 'Successful Completion';
  ['01'] = 'Warning';
  ['02'] = 'No Data (this is also a warning class per the SQL standard)';
  ['03'] = 'SQL Statement Not Yet Complete';
  ['08'] = 'Connection Exception';
  ['09'] = 'Triggered Action Exception';
  ['0A'] = 'Feature Not Supported';
  ['0B'] = 'Invalid Transaction Initiation';
  ['0F'] = 'Locator Exception';
  ['0L'] = 'Invalid Grantor';
  ['0P'] = 'Invalid Role Specification';
  ['0Z'] = 'Diagnostics Exception';
  ['20'] = 'Case Not Found';
  ['21'] = 'Cardinality Violation';
  ['22'] = 'Data Exception';
  ['23'] = 'Integrity Constraint Violation';
  ['24'] = 'Invalid Cursor State';
  ['25'] = 'Invalid Transaction State';
  ['26'] = 'Invalid SQL Statement Name';
  ['27'] = 'Triggered Data Change Violation';
  ['28'] = 'Invalid Authorization Specification';
  ['2B'] = 'Dependent Privilege Descriptors Still Exist';
  ['2D'] = 'Invalid Transaction Termination';
  ['2F'] = 'SQL Routine Exception';
  ['34'] = 'Invalid Cursor Name';
  ['38'] = 'External Routine Exception';
  ['39'] = 'External Routine Invocation Exception';
  ['3B'] = 'Savepoint Exception';
  ['3D'] = 'Invalid Catalog Name';
  ['3F'] = 'Invalid Schema Name';
  ['40'] = 'Transaction Rollback';
  ['42'] = 'Syntax Error or Access Rule Violation';
  ['44'] = 'WITH CHECK OPTION Violation';
  ['53'] = 'Insufficient Resources';
  ['54'] = 'Program Limit Exceeded';
  ['55'] = 'Object Not In Prerequisite State';
  ['57'] = 'Operator Intervention';
  ['58'] = 'System Error (errors external to PostgreSQL itself)';
  ['F0'] = 'Configuration File Error';
  ['HV'] = 'Foreign Data Wrapper Error (SQL/MED)';
  ['P0'] = 'PL/pgSQL Error';
  ['XX'] = 'Internal Error';
}

function PGServerError:__init(t)
  self._t = t
  assert(t.S)
  assert(t.C)
  assert(t.M)
  return self
end

function PGServerError:no()   return self._t.C end

function PGServerError:name() return self._t.C end

function PGServerError:msg()  return self._t.M end

function PGServerError:cat()  return ERROR_PG  end

function PGServerError:class()
  local c = self._t.C:sub(1,2)
  return c, CLASS[c] or "Unknown"
end

function PGServerError:__tostring()
  local F = string.format

  local str, t = {}, self._t
  local cno, cname = self:class()

  append(str, F("[%s][%s][%s]%s", ERROR_PG, t.S, t.C, t.M))
  if t.D then append(str, t.D)                             end
              append(str, F("Class: %s",           cname))
  if t.H then append(str, F("Hint: %s",              t.H)) end
  if t.P then append(str, F("Position: %s",          t.P)) end
  if t.p then append(str, F("Internal position: %s", t.p)) end
  if t.q then append(str, F("Internal query: %s",    t.q)) end
  if t.W then append(str, F("Where: %s",             t.W)) end
  if t.s then append(str, F("Schema query: %s",      t.s)) end
  if t.t then append(str, F("Table: %s",             t.t)) end
  if t.c then append(str, F("Column: %s",            t.c)) end
  if t.d then append(str, F("Data type: %s",         t.d)) end
  if t.n then append(str, F("Constraint: %s",        t.n)) end
  if t.F then append(str, F("File: %s",              t.F)) end
  if t.L then append(str, F("Line: %s",              t.L)) end
  if t.R then append(str, F("Routine: %s",           t.R)) end

  return table.concat(str, '\n')
end

end

local PGProtoError = ut.class() do

function PGProtoError:__init(state, event, data)
  self._fsm   = fsm
  self._state = state
  self._event = event
  self._ext   = data
  return self
end

function PGProtoError:no()   return -1       end

function PGProtoError:name() return "EPROTO" end

function PGProtoError:msg()
  return string.format("Unexpected message `%s` in state `%s`",
    self._event, self._state)
end

function PGServerError:cat() return ERROR_PG end

function PGProtoError:ext() return self._ext end

function PGProtoError:__tostring()
  return string.format("[%s][%s] %s (%d) - `%q`",
    ERROR_PG, self:name(), self:msg(), self:no(), self:ext()
  )
end

end

return {
  ServerError = PGServerError;
  ProtoError  = PGProtoError;
}