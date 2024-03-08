cjson.encode_invalid_numbers(false)

local asks_to_push = {}
local bids_to_push = {}

local gexchange = ''
local gpair = ''

local precision = 1000000000000

local bids_key = "sob:"..gexchange..":"..gpair..":bids"
local asks_key = "sob:"..gexchange..":"..gpair..":asks"

local function convert_to(price)
    return tonumber(price)*precision
end

local function convert_from(price)
    return tostring(price/precision)
end

local call_in_chunks = function (command, key, args)
    local step = 300
    for i = 1, #args, step do
        redis.call(command, key, unpack(args, i, math.min(i + step - 1, #args)))
    end
end

local function flushNonEmptyOutdatedOrders(price, isAsks, timestamp)
    local matched_orders = {}
    local key = ''
    local side = ''
    if (isAsks == true) then
        side = 'bids'
        key = bids_key
        matched_orders = redis.call('ZRANGEBYSCORE', bids_key, price, "+inf")
    else
        side = 'asks'
        key = asks_key
        matched_orders = redis.call('ZRANGEBYSCORE', asks_key, "-inf", price)
    end
    local have_new_orders = false
    local orders_to_remove = {}
    local orders_to_remove_decoded = {}
    local orders_to_keep = {}
    for i,orderjson in ipairs(matched_orders) do
        local order = cjson.decode(orderjson)
        local order_timestamp = tonumber(order[3])
        if (order_timestamp < timestamp) then
            table.insert(orders_to_remove, orderjson)
            table.insert(orders_to_remove_decoded, order)
        else
            have_new_orders = true
            table.insert(orders_to_keep, order)
            table.insert(orders_to_keep, orderjson)
        end
    end

    if (table.getn(orders_to_remove) > 0) then
        call_in_chunks('ZREM', key, orders_to_remove)

        for i, order in ipairs(orders_to_remove_decoded) do         
            redis.call('PUBLISH', "sob:"..gexchange..":"..gpair.."", "{\"pair\":\""..gpair.."\",\"exchange\":\""..gexchange.."\",\"id\":\"0\",\"size\":\"0\",\"price\":\""..order[2].."\",\"side\":\""..side.."\",\"timestamp\":"..order[3].."}")
        end
    end

    return {
        status = have_new_orders,
        orders_to_keep = orders_to_keep
    }
end

local function dedupe_order(id, exchange, pair,
                            side, price, amount, timestamp)
        -- local new_order = redis.call('SET', id, 1, 'NX', 'EX', 5);
        -- if not new_order then
        --     return 0;
        -- end

        local order = {
            id = id, 
            exchange = exchange, 
            pair = pair, 
            side = side,
            price = price, 
            amount = tonumber(amount), 
            timestamp = timestamp
        }
        gexchange = exchange
        gpair = pair

        if (order.side == 'ask') then
            table.insert(asks_to_push, order)
        else
            table.insert(bids_to_push, order)
        end
        return 1;
end

local actorlist = ARGV
for i, k in ipairs(actorlist) do
    if (i > 1 and i % 7 == 2) then
        dedupe_order(actorlist[i], actorlist[i+1], actorlist[i+2], actorlist[i+3], actorlist[i+4], actorlist[i+5], actorlist[i+6])
    end
end

local function process_orders(key, orders, isAsks)
    local minmax = {min = 1, max = 1}
    local pricesDict = {}
    local pricesDictToIgnore = {}
    local keys_to_add = {}
    for i, order in ipairs(orders) do
        pricesDict[convert_to(order.price)] = order
        if convert_to(order.price) > convert_to(orders[minmax.max].price) then  
            minmax.max = i
        end
        if convert_to(order.price) < convert_to(orders[minmax.min].price) then  
            minmax.min = i
        end
    end

    local min = convert_to(orders[minmax.min].price)
    local max = convert_to(orders[minmax.max].price)

    local exitsting_orders = redis.call('ZRANGEBYSCORE', key, min, max, 'WITHSCORES')
    local keys_to_remove = {}
    local keys_to_remove_decoded = {}
    for i, order in ipairs(exitsting_orders) do
        if (i % 2 == 1) then
            local decoded_order = cjson.decode(order)
            if (tonumber(decoded_order[0]) == 0.0 or tonumber(decoded_order[0]) == 0) then
                table.insert(keys_to_remove, order)
                table.insert(keys_to_remove_decoded, cjson.decode(order))
            end
            if pricesDict[tonumber(exitsting_orders[i+1])] then
                if  (tonumber(pricesDict[tonumber(exitsting_orders[i+1])].timestamp) == 0.0) or
                    (tonumber(pricesDict[tonumber(exitsting_orders[i+1])].timestamp) > tonumber(decoded_order[3])) then
                    table.insert(keys_to_remove, order)
                    table.insert(keys_to_remove_decoded, cjson.decode(order))
                    else
                    pricesDictToIgnore[tonumber(exitsting_orders[i+1])] = true
                end
            end
        end
    end

    local search_for_zeros = redis.call('ZRANGEBYSCORE', key, max, '+inf')
    for i, order in ipairs(search_for_zeros) do
        local decoded_order2 = cjson.decode(order)
        if (tonumber(decoded_order2[0]) == 0.0 or tonumber(decoded_order2[0]) == 0) then
            table.insert(keys_to_remove, order)
            table.insert(keys_to_remove_decoded, decoded_order2)
        else
            break
        end
    end

    if (table.getn(keys_to_remove) > 0) then
        redis.call('ZREM', key, unpack(keys_to_remove))   
        local side = ''
        if isAsks then
            side = 'asks'
        else
            side = 'bids'
        end
        for i, order in ipairs(keys_to_remove_decoded) do         
            redis.call('PUBLISH', "sob:"..gexchange..":"..gpair.."", "{\"pair\":\""..gpair.."\",\"exchange\":\""..gexchange.."\",\"id\":\"0\",\"size\":\"0\",\"price\":\""..order[2].."\",\"side\":\""..side.."\",\"timestamp\":"..order[3].."}")
        end
    end

    local response = {status = false}
    local opposite_key = ''
    
    if isAsks then
        response = flushNonEmptyOutdatedOrders(min, isAsks, tonumber(orders[minmax.min].timestamp))
        opposite_key = bids_key
    else
        response = flushNonEmptyOutdatedOrders(max, isAsks, tonumber(orders[minmax.max].timestamp))
        opposite_key = asks_key
    end

    if (response.status) then
        if isAsks then
            pricesDictToIgnore[orders[minmax.min].price] = true
        else
            pricesDictToIgnore[orders[minmax.max].price] = true
        end
    end

    for i, order in ipairs(orders) do
        if not pricesDictToIgnore[order.price] then
            if (tonumber(order.amount) > 0.0) then
                table.insert(keys_to_add, convert_to(order.price))
                table.insert(keys_to_add, "[\""..order.amount.."\",\""..order.price.."\",\""..order.timestamp.."\"]")
            end
            redis.call('PUBLISH', "sob:"..order.exchange..":"..order.pair.."", "{\"pair\":\""..order.pair.."\",\"exchange\":\""..order.exchange.."\",\"id\":\""..order.id.."\",\"size\":\""..order.amount.."\",\"price\":\""..order.price.."\",\"side\":\""..order.side.."\",\"timestamp\":"..order.timestamp.."}")
        end
    end


    return {
        keys_to_add = keys_to_add
    }
end

local function add_orders_to_orderboook(orders, isAsks)
    local key = ''
    if (isAsks) then
        key = asks_key
        redis.call('ZREMRANGEBYRANK', key, 150, 200)
    else
        key = bids_key
        redis.call('ZREMRANGEBYRANK', key, -200, -150)
    end

    local data = process_orders(key, orders, isAsks)

    if (table.getn(data.keys_to_add) > 0) then
        call_in_chunks('ZADD', key, data.keys_to_add)
    end
end

bids_key = "sob:"..gexchange..":"..gpair..":bids"
asks_key = "sob:"..gexchange..":"..gpair..":asks"
local timekey = ARGV[1]

if (table.getn(asks_to_push) > 0) then
    redis.call('INCRBY', timekey, table.getn(asks_to_push))
    redis.call('EXPIRE', timekey, 10)
    add_orders_to_orderboook(asks_to_push, true)
end

if (table.getn(bids_to_push) > 0) then
    redis.call('INCRBY', timekey, table.getn(bids_to_push))
    redis.call('EXPIRE', timekey, 10)
    add_orders_to_orderboook(bids_to_push, false)
end
