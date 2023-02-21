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