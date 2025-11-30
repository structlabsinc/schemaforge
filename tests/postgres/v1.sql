CREATE TABLE public.users (
    id SERIAL PRIMARY KEY,
    profile JSONB,
    tags TEXT[],
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE orders (
    id UUID PRIMARY KEY,
    amount DECIMAL(10, 2)
);
