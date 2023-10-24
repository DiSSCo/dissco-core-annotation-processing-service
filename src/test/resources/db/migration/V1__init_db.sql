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