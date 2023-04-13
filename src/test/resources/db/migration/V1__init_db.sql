CREATE TABLE public.new_annotation (
	id text NOT NULL,
	"version" int4 NOT NULL,
	"type" text NOT NULL,
	motivation text NOT NULL,
	target_id text NOT NULL,
	target_field text NULL,
	target_body jsonb NOT NULL,
	body jsonb NOT NULL,
	preference_score int4 NOT NULL,
	creator text NOT NULL,
	created timestamptz NOT NULL,
	generator_id text NOT NULL,
	generator_body jsonb NOT NULL,
	"generated" timestamptz NOT NULL,
	last_checked timestamptz NOT NULL,
	deleted timestamptz NULL,
	CONSTRAINT new_annotation_pk PRIMARY KEY (id)
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