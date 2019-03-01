
-- 计算每天有多少的pv、uv、订单量和收入以及注册用户数【register事件的不重复用户数】
-- pv直接查询 active_name为pageview的记录；uv表示用户访问量，对pageview的那些记录进行用户去重
-- 首先计算 2018-05-29 这一天的，熟悉题目; 然后将where条件下的day='2018-05-29'换到group by
-- active_name [order, pageview, pay, register]
select 
  count(1) as pv, day
from weblog
where active_name='pageview'
group by day

select count(user_id), day
from
(
    select
      user_id, day
    from weblog
    where active_name='pageview'
    group by day, user_id
) t1
group by day


-- 订单量和收入
select count(1) order_num, sum(pay_amount) total_pay, day
from
(
    select user_id
    from weblog
    where active_name='pay' and day='2018-05-29'
) t1
-- 一天的weblog中有用户肯定比orders表中要少
left outer join orders on t1.user_id=orders.user_id

-- 注册用户数
select count(1) register_user_num, day
from weblog
where active_name='register' and day='2018-05-29'


-- 最后按照日期join在一起
-- 9个jobs
create table sale_by_day as
select pv, uv, order_num, total_pay, register_user_num, t1.day
from
    (
    -- 计算每天的pv
    select count(1) as pv, day
    from weblog
    where active_name='pageview'
    group by day
    ) t1 
join 
    (
    -- 计算每天的uv
    select count(user_id) as uv, day
    from
        (
        select
        user_id, day
        from weblog
        where active_name='pageview'
        group by day, user_id
        ) t1
    group by day
    ) t2 on t1.day=t2.day
join
    (
    -- 计算每天的订单数量，总收入
    select count(1) order_num, sum(pay_amount) total_pay, weblog.day
    from orders
    join weblog on weblog.user_id=orders.user_id and weblog.active_name='pay'
    group by weblog.day
    ) t3 on t1.day=t3.day 
join
    (
    -- 计算每天的注册用户
    select count(1) register_user_num, day
    from weblog
    where active_name='register'
    group by day
    ) t4 on t1.day=t4.day

-----------------------------------------------------------------------

-- 2 计算访问product页面的用户，有多少比例在30分钟内下单并且支付成功，并查询对应的商品
-- 访问product页面的用户多少人、30分钟内下单并支付成功的多少人、product_id

-- step1: 从会话开始，30分钟内下单且支付成功的user_id, product_id
--    需要查询到用户刚刚进来的着陆时间，支付时间
--    进一步筛选出30分钟内下单的user_id, product_id
select 
    user_id, 
    time_tag, 
    -- 通过pay前面2个页面才是下单的产品网页，得到产品id
    regexp_extract(product_url, '.*/product/([0-9]+).*', 1) as product_id,  
    landing_time
from
(
    select 
      user_id,
      time_tag, 
      active_name,
        -- 用户着陆时间
      first_value(time_tag) over (partition by session_id order by time_tag asc) as landing_time,
      lag(req_url, 2) over (partition by session_id order by time_tag asc) as product_url,
        -- 去重
      row_number() over (partition by user_id) as row_num
    from weblog
) t
  -- 选出30分钟内付款的下单用户
where t.active_name='pay' and t.time_tag-t.landing_time<=30*60*1000 and t.row_num=1

-- step2: 查询访问product页面的用户总数【同一个用户访问了多次product页面，只算一次】
select count(user_id)
from
(
    select user_id
    from weblog
    where req_url regexp '.*/product/([0-9]+).*'  -- req_url是product页面
    group by user_id
) t

-- step3: 通过笛卡尔积连接起来
select t1.user_id, t1.time_tag, t1.product_id, t1.landing_time, t2.total_product_user
from
(
    select 
        user_id, 
        time_tag, 
        regexp_extract(product_url, '.*/product/([0-9]+).*', 1) as product_id,
        landing_time
    from
    (
        select 
        user_id,
        time_tag, 
        active_name,
            -- 用户着陆时间
        first_value(time_tag) over (partition by session_id order by time_tag asc) as landing_time,
        lag(req_url, 2) over (partition by session_id order by time_tag asc) as product_url,
            -- 去重
        row_number() over (partition by user_id) as row_num
        from weblog
    ) t
    -- 选出30分钟内付款的下单用户
    where t.active_name='pay' and t.time_tag-t.landing_time<=30*60*1000 and t.row_num=1
) t1
join
(
    select count(user_id) total_product_user
    from
    (
        select user_id
        from weblog
        where req_url regexp '.*/product/([0-9]+).*'  -- req_url是product页面
        group by user_id
    ) t
) t2




