CREATE TABLE users (
    id BIGINT PRIMARY KEY CHECK (id > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE segments (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code VARCHAR(64) NOT NULL UNIQUE CHECK (code ~ '^[A-Z][A-Z0-9_]{0,63}$'),
    name VARCHAR(200) NOT NULL CHECK (length(btrim(name)) > 0),
    description TEXT NOT NULL DEFAULT '' CHECK (length(description) <= 4000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE user_segments (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    segment_id BIGINT NOT NULL REFERENCES segments(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, segment_id)
);
CREATE INDEX user_segments_segment_idx ON user_segments(segment_id);
CREATE FUNCTION prevent_segment_code_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.code IS DISTINCT FROM OLD.code THEN
        RAISE EXCEPTION 'segment code is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER segments_immutable_code BEFORE UPDATE OF code ON segments
FOR EACH ROW EXECUTE FUNCTION prevent_segment_code_change();
