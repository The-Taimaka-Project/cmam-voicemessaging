-- "data".calls definition

-- Drop table

-- DROP TABLE "data".calls;

CREATE TABLE "data".calls (
	uuid uuid NOT NULL,
	pid text NOT NULL,
	"date" date NOT NULL,
	call_type text NOT NULL,
	site text NOT NULL,
	"language" text NOT NULL,
	actual_call bool NOT NULL,
	morning_answer bool NOT NULL,
	morning_endstatus text NULL,
	morning_duration numeric NULL,
	afternoon_answer bool NULL,
	afternoon_endstatus text NULL,
	afternoon_duration numeric NULL,
	phone text NOT NULL,
	note text NULL,
	curr_call text NOT NULL,
	morning_endtime timestamptz NULL,
	afternoon_endtime timestamptz NULL,
	morning_audio_file_name text NULL,
	morning_audio_file_existed bool NULL,
	phone_type text NOT NULL,
	afternoon_audio_file_name text NULL,
	afternoon_audio_file_existed bool NULL,
	morning_at_queue_status text NULL,
	afternoon_at_queue_status text NULL,
	morning_at_queued bool NULL,
	afternoon_at_queued bool NULL,
	CONSTRAINT calls_pkey PRIMARY KEY (uuid)
);