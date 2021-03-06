local _M = {}
local _meta = { __index=_M }

local logging = require( "acid.logging" )

local log_enabled_phases = {
    set=true,
    rewrite=true,
    access=true,
    content=true,
    header_filter=true,
    body_filter=true,
    log=true,
}

function _M.new(opt)
    local e = {
        tracking_varname = opt.tracking_varname
    }
    setmetatable( e, _meta )
    return e
end

function _M:_log(offset, ...)
    ngx.log( ngx.ERR, logging.tostr(...) )
end
function _M:logerr(...)
    self:_log(1, ...)
end

function _M:track(...)

    local p = ngx.get_phase()

    if not log_enabled_phases[p] then
        return
    end

    local nv = ngx.var
    local vname = self.tracking_varname
    if vname == nil then
        return
    end

    if nv[vname] == nil then
        return
    end
    local s = logging.tostr(...)

    if nv[vname] == "" then
        nv[vname] = s
    else
        nv[vname] = nv[vname] .. ', ' .. s
    end
end

return _M
