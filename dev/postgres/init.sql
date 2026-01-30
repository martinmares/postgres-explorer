-- Schema
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS public.customers (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    full_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    country TEXT NOT NULL,
    vip BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS public.products (
    id BIGSERIAL PRIMARY KEY,
    sku TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price_cents INT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS sales.orders (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES public.customers(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL,
    total_cents INT NOT NULL
);

CREATE TABLE IF NOT EXISTS sales.order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES sales.orders(id),
    product_id BIGINT NOT NULL REFERENCES public.products(id),
    quantity INT NOT NULL,
    price_cents INT NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.page_views (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES public.customers(id),
    url TEXT NOT NULL,
    viewed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    referrer TEXT
);

CREATE INDEX IF NOT EXISTS idx_customers_country ON public.customers(country);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON sales.orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON sales.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON sales.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_page_views_viewed_at ON analytics.page_views(viewed_at);

-- Seed data
INSERT INTO public.customers (email, full_name, country, vip)
SELECT
  'user' || g || '@example.com',
  'Customer ' || g,
  (ARRAY['CZ','SK','DE','AT','PL','US'])[1 + (g % 6)],
  (g % 10 = 0)
FROM generate_series(1, 500) AS g;

INSERT INTO public.products (sku, name, category, price_cents, active)
SELECT
  'SKU-' || g,
  'Product ' || g,
  (ARRAY['books','electronics','apparel','home'])[1 + (g % 4)],
  500 + (g * 13) % 20000,
  (g % 20 <> 0)
FROM generate_series(1, 200) AS g;

INSERT INTO sales.orders (customer_id, created_at, status, total_cents)
SELECT
  1 + (g % 500),
  now() - (g || ' hours')::interval,
  (ARRAY['new','paid','shipped','cancelled'])[1 + (g % 4)],
  1000 + (g * 73) % 50000
FROM generate_series(1, 2000) AS g;

INSERT INTO sales.order_items (order_id, product_id, quantity, price_cents)
SELECT
  1 + (g % 2000),
  1 + (g % 200),
  1 + (g % 5),
  200 + (g * 19) % 20000
FROM generate_series(1, 6000) AS g;

INSERT INTO analytics.page_views (customer_id, url, viewed_at, referrer)
SELECT
  1 + (g % 500),
  '/product/' || (1 + (g % 200)),
  now() - (g || ' minutes')::interval,
  (ARRAY['google','newsletter','direct','partner'])[1 + (g % 4)]
FROM generate_series(1, 8000) AS g;

-- Vytvoř tabulku pro audit log
CREATE TABLE IF NOT EXISTS sales.order_audit (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    action TEXT NOT NULL,
    changed_by TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    old_status TEXT,
    new_status TEXT
);

-- Audit trigger function pro změny status v orders
CREATE OR REPLACE FUNCTION sales.audit_order_status_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO sales.order_audit (order_id, action, changed_by, old_status, new_status)
        VALUES (NEW.id, 'status_change', current_user, OLD.status, NEW.status);
    ELSIF TG_OP = 'INSERT' THEN
        INSERT INTO sales.order_audit (order_id, action, changed_by, new_status)
        VALUES (NEW.id, 'created', current_user, NEW.status);
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO sales.order_audit (order_id, action, changed_by, old_status)
        VALUES (OLD.id, 'deleted', current_user, OLD.status);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger pro audit orders
CREATE TRIGGER trg_audit_order_changes
    AFTER INSERT OR UPDATE OR DELETE ON sales.orders
    FOR EACH ROW
    EXECUTE FUNCTION sales.audit_order_status_changes();

-- Vytvoř tabulku pro email notifikace
CREATE TABLE IF NOT EXISTS public.customer_emails (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES public.customers(id),
    email_type TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    subject TEXT
);

-- Function pro automatické poslání welcome emailu
CREATE OR REPLACE FUNCTION public.send_welcome_email()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO public.customer_emails (customer_id, email_type, subject)
    VALUES (NEW.id, 'welcome', 'Welcome to our platform!');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger před vložením nového zákazníka
CREATE TRIGGER trg_send_welcome_email
    AFTER INSERT ON public.customers
    FOR EACH ROW
    EXECUTE FUNCTION public.send_welcome_email();

-- Function pro validaci emailu
CREATE OR REPLACE FUNCTION public.validate_customer_email()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN
        RAISE EXCEPTION 'Invalid email format: %', NEW.email;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- BEFORE trigger pro validaci emailu
CREATE TRIGGER trg_validate_email
    BEFORE INSERT OR UPDATE ON public.customers
    FOR EACH ROW
    EXECUTE FUNCTION public.validate_customer_email();

-- Function pro aktualizaci timestampu
CREATE OR REPLACE FUNCTION sales.update_order_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.created_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Disabled trigger (příklad vypnutého triggeru)
CREATE TRIGGER trg_update_timestamp
    BEFORE UPDATE ON sales.orders
    FOR EACH ROW
    EXECUTE FUNCTION sales.update_order_timestamp();

-- Vypni tento trigger jako ukázku
ALTER TABLE sales.orders DISABLE TRIGGER trg_update_timestamp;

ANALYZE;
