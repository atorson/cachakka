CREATE TYPE cachakka.cache_index_payload (
  index_values list<blob>
);

CREATE TABLE IF NOT EXISTS cachakka.cache_index_snapshot (
    cache_shard text,
    cache_index text,
    id text,
    payload frozen <cache_index_payload>,
    PRIMARY KEY ((cache_shard, cache_index), id)
);