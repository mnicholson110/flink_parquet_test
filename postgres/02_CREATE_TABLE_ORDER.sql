CREATE TABLE order_schema.order (
  order_id SERIAL PRIMARY KEY,
  --order_amount NUMERIC(10,2) NOT NULL,
  --order_status_id INTEGER NOT NULL,
  --store_id INTEGER NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
