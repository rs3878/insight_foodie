drop table if exists cf;
create table cf as (
    select user_int_id, bus_id, review_stars, review_date
from (
    (select user_ids, user_int_id from clean_user) u
join
    (select user_ids, business_ids, review_stars, review_date
    from clean_review
    where business_ids in(
        select business_ids from clean_business
        )
    ) r
on u.user_ids = r.user_ids)
join
    (select bus_id, business_ids from clean_business) b
on b.business_ids = r.business_ids
);

--update cf
--set review_stars = review_stars-2.5;



create table user_location as (
with tmp as (
    select user_ids,review_ids,clean_review.business_ids,business_city
    from clean_review
    join clean_business
    on clean_review.business_ids=clean_business.business_ids
), tmp2 as (
    select count(business_city) as cnt, business_city, user_ids
    from tmp
    group by business_city, user_ids
), tmp3 as (
    select row_number() over (partition by user_ids order by cnt desc) rn , user_ids, business_city
    from tmp2
) select user_ids, business_city as user_city from tmp3
    where rn =1
);


