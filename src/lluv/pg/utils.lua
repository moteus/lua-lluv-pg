local function append(t, v) t[#t + 1] = v end

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

local function super(class, self, m, ...)
  if class.__base and class.__base[m] then
    return class.__base[m](self, ...)
  end
  if m == '__init' then
    return self
  end
end

local function call_q(q, ...)
  while true do
    local cb = q:pop()
    if not cb then break end
    cb(...)
  end
end

local function is_callable(f)
  return (type(f) == 'function') and f
end

return {
  append      = append;
  deep_copy   = deep_copy;
  super       = super;
  is_callable = is_callable;
  call_q      = call_q;
}