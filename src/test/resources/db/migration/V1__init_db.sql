create table annotation
(
    id               text                     not null
        constraint annotation_pk
            primary key,
    version          integer                  not null,
    type             text                     not null,
    motivation       text                     not null,
    motivated_by     text,
    target_id        text                     not null,
    target           jsonb                    not null,
    body             jsonb                    not null,
    creator_id       text                     not null,
    creator          jsonb                    not null,
    created          timestamp with time zone not null,
    generator        jsonb                    not null,
    generated        timestamp with time zone not null,
    last_checked     timestamp with time zone not null,
    aggregate_rating jsonb,
    deleted_on       timestamp with time zone,
    annotation_hash  uuid,
    mjr_job_id       text,
    batch_id         uuid
);

create type job_state as enum ('SCHEDULED', 'RUNNING', 'FAILED', 'COMPLETED');
create type mjr_target_type as enum ('DIGITAL_SPECIMEN', 'MEDIA_OBJECT');
create type error_code as enum ('TIMEOUT', 'DISSCO_EXCEPTION');

create table mas_job_record
(
    job_id             text                     not null
        constraint mas_job_record_pk
            primary key,
    job_state          job_state                not null,
    mas_id             text                     not null,
    time_started       timestamp with time zone not null,
    time_completed     timestamp with time zone,
    annotations        jsonb,
    target_id          text                     not null,
    user_id            text,
    target_type        mjr_target_type,
    batching_requested boolean,
    error              error_code,
    time_to_live       timestamp with time zone
);

create table annotation_batch_record
(
    batch_id             uuid                     not null
        constraint annotation_batch_pk
            primary key,
    creator_id           text                     not null,
    generator_id         text,
    parent_annotation_id text                     not null,
    created_on           timestamp with time zone not null,
    last_updated         timestamp with time zone,
    job_id               text
        constraint annotation_batch_fk
            references mas_job_record,
    batch_quantity       bigint
);

