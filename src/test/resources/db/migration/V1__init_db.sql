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
    deleted_on       timestamp with time zone
);

CREATE TABLE public.handles (
	handle bytea NOT NULL,
	idx int4 NOT NULL,
	"type" bytea NULL,
	"data" bytea NULL,
	ttl_type int2 NULL,
	ttl int4 NULL,
	"timestamp" int8 NULL,
	refs text NULL,
	admin_read bool NULL,
	admin_write bool NULL,
	pub_read bool NULL,
	pub_write bool NULL,
	CONSTRAINT handles_pkey PRIMARY KEY (handle, idx)
);
CREATE INDEX dataindex ON public.handles USING btree (data);
CREATE INDEX handleindex ON public.handles USING btree (handle);

CREATE
EXTENSION IF NOT EXISTS "uuid-ossp";

create table mas_job_record
(
    job_id         uuid default uuid_generate_v4() not null
        constraint mas_job_record_pk
            primary key,
    state          text                            not null,
    creator_id     text                            not null,
    time_started   timestamp with time zone        not null,
    time_completed timestamp with time zone,
    annotations    jsonb,
    target_id      text                            not null
);

create index mas_job_record_created_idx
    on mas_job_record (time_started);

create index mas_job_record_job_id_index
    on mas_job_record (job_id);
