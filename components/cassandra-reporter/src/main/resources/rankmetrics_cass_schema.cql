CREATE TABLE IF NOT EXISTS cachakka.group_rank_metric (
    shard text,
    updated_at timestamp,
    group_rank_name text,
    group_member_id text,
    group_member_name text,
    full_metric_id text,
    metric_value double,
    PRIMARY KEY ((shard, group_rank_name), group_member_id, full_metric_id)
);


