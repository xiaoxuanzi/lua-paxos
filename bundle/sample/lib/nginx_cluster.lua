local acid_cluster = require( "acid.cluster" )
local impl_ngx = require( "acid.impl_ngx" )

local _M = {}

local function make_loop_member_creator(idents)

    local i = 0

    return function(impl, paxos, dead_ident, members)
        for ii = 1, #idents do
            i = i + 1
            local new_id = idents[(i % #idents) + 1]
            if members[new_id] == nil then
                return {[new_id]=true}
            end
        end
        return nil, 'NoFreeMember'
    end
end

local function init_cluster_check(cluster, member_id, interval)

    local checker

    checker = function(premature)

        if premature then
            return
        end

        cluster:member_check(member_id)

        local ok, err = ngx.timer.at(interval, checker)
    end
    local ok, err = ngx.timer.at( 0, checker )
end

function _M.new(opt)
    -- opt = {
    --  path = 'paxos data path',
    --  check_interval = 5,
    --  new_member = function() end,
    -- }

    local check_interval = opt.check_interval or 5
    local dead_wait = opt.dead_wait or 60

    local impl = impl_ngx.new({})

    impl.sto_base_path = opt.path

    if opt.new_member ~= nil then
        impl.new_member = opt.new_member
    elseif opt.standby ~= nil then
        impl.new_member = make_loop_member_creator(opt.standby)
    else
        impl.new_member = function()
            return nil, 'NoFreeMember'
        end
    end

    local cluster = acid_cluster.new(impl, {
        dead_wait = dead_wait,
        admin_lease = opt.admin_lease or (check_interval*2) or 60,
        max_dead = opt.max_dead or 1,
    })

    local mid = {cluster_id=opt.cluster_id, ident=opt.ident}
    init_cluster_check(cluster, mid, check_interval)

    return cluster
end

return _M
