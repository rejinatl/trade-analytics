CREATE TABLE IF NOT EXISTS public.trade_order_tracking
(
	id SERIAL PRIMARY KEY,
    msg_seq_num bigint NOT NULL,
    order_id character varying(500) COLLATE pg_catalog."default" NOT NULL,
    instrument character varying(500) COLLATE pg_catalog."default" NOT NULL,
    account character varying(255) COLLATE pg_catalog."default" NOT NULL,
    side integer NOT NULL DEFAULT 1,
    price numeric(10,2) NOT NULL,
    display_qty integer DEFAULT 0,
    last_qty integer DEFAULT 0,
    message_type character varying(200) COLLATE pg_catalog."default",
    created_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    msg_datetime timestamp with time zone
)