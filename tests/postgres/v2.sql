CREATE TABLE public.users (
    id SERIAL PRIMARY KEY,
    profile JSONB,
    tags TEXT[],
    active BOOLEAN DEFAULT FALSE, -- Modified default
    last_login TIMESTAMP
);

CREATE TABLE orders (
    id UUID PRIMARY KEY,
    amount DECIMAL(10, 2),
    status VARCHAR(50)
);

CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    event_data JSONB
);
