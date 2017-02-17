local ut = require "lluv.utils"

local function deep_copy(t)
  local o = {}
  for k, v in pairs(t) do
    if type(v) == "table" then
      o[k] = deep_copy(v)
    else
      o[k] = v
    end
  end
  return o
end

local FSM = ut.class() do

function FSM:__init(state)
  self._actions = {}
  self._states  = {}
  self._start_name = state
  self._state   = nil

  return self
end

function FSM:step(event, ...)
  if type(self._state) ~= 'table' then
    -- invalid state
    return
  end

  local f = self._state[event] or self._state['*']
  if not f then
    -- invalid transition
    return
  end

  local action = f[1] and self._actions[f[1]]

  local state_name, state
  if f[2] then
    state_name = f[2]
    state = self._states[state_name]
    assert(state, "Unknown state:" .. state_name)
  else
    state_name = self._state_name
    assert(self._states[self._state_name] == self._state)
    state = self._state
  end

  -- this is finish state
  if type(state) == "function" then
    if action then state(self, event, action(self, event, ...))
    else state(self, event, ...) end

    self._state, self._state_name = nil
    return
  end

  if action then action(self, event, ...) end

  -- transition
  self._state, self._state_name = state, state_name

  action = state[1] and self._actions[state[1]]

  if action then action(self, event, ...) end

  return self
end

function FSM:action(name, fn)
  self._actions[name] = fn
end

function FSM:state(name, v)
  self._states[name] = v
end

function FSM:start(state)
  self._state_name = state or self._start_name
  self._state = self._states[self._state_name]

  return self
end

function FSM:reset()
  return self:start()
end

function FSM:clone()
  local fsm = FSM.new()
  fsm._actions    = deep_copy(self._actions)
  fsm._states     = deep_copy(self._states)
  fsm._start_name = self._start_name

  return fsm
end

function FSM:active()
  return self._state_name
end

end

return FSM
