create type annotation_status_enum as enum('ACCEPTED', 'REJECTED', 'PENDING');

create table annotation
(
    id              text                     not null
        constraint new_annotation_pk
            primary key,
    version         integer                  not null,
    type            text                     not null,
    annotation_hash uuid,
    motivation      text                     not null,
    mjr_job_id      text,
    batch_id        uuid,
    creator         text                     not null,
    created         timestamp with time zone not null,
    modified        timestamp with time zone not null,
    last_checked    timestamp with time zone not null,
    tombstoned      timestamp with time zone,
    target_id       text                     not null,
    data            jsonb,
    annotation_status annotation_status_enum
);

create type job_state as enum ('SCHEDULED', 'RUNNING', 'FAILED', 'COMPLETED');
create type mjr_target_type as enum ('DIGITAL_SPECIMEN', 'MEDIA_OBJECT');
create type error_code as enum ('TIMEOUT', 'DISSCO_EXCEPTION', 'MAS_EXCEPTION');

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
    creator            text,
    target_type        mjr_target_type,
    batching_requested boolean,
    error              error_code,
    error_message      text,
    time_to_live       timestamp with time zone
);

create table annotation_batch_record
(
    id                   uuid                     not null
        constraint annotation_batch_pk
            primary key,
    creator              text                     not null,
    created              timestamp with time zone not null,
    last_updated         timestamp with time zone,
    job_id               text,
    batch_quantity       bigint,
    parent_annotation_id text
);

