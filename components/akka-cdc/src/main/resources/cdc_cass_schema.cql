CREATE TABLE IF NOT EXISTS cachakka.cdc_delta (
    shard_id text,
    bucket int,
    seq_number int,
    blob blob,
    PRIMARY KEY ((shard_id, bucket), seq_number)
);

CREATE TABLE IF NOT EXISTS cachakka.cdc_flush_state (
    shard_id text,
    bucket int,
    seq_number int,
    PRIMARY KEY (shard_id)
);

CREATE TABLE IF NOT EXISTS cachakka.cdc_processing_bucket (
    shard_id text,
    active_bucket int,
    PRIMARY KEY (shard_id)
);

CREATE TABLE IF NOT EXISTS cachakka.cdc_processing_status (
    shard_id text,
    bucket int,
    id text,
    processing_state int,
    PRIMARY KEY ((shard_id,bucket), id)
);

