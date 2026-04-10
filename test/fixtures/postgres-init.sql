-- Test schema for PostgreSQL adapter integration tests

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    tenant_id VARCHAR(50) NOT NULL,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE users IS 'User accounts table';
COMMENT ON COLUMN users.id IS 'Unique user identifier';
COMMENT ON COLUMN users.email IS 'User email address';
COMMENT ON COLUMN users.tenant_id IS 'Tenant identifier for isolation';

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    tenant_id VARCHAR(50) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE orders IS 'Customer orders';
COMMENT ON COLUMN orders.status IS 'Order status: pending, completed, cancelled';

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_tenant_id ON orders(tenant_id);
CREATE INDEX idx_orders_status ON orders(status);

-- Insert test data
INSERT INTO users (email, name, tenant_id, active) VALUES
    ('alice@example.com', 'Alice Smith', 'tenant-1', true),
    ('bob@example.com', 'Bob Jones', 'tenant-1', true),
    ('charlie@example.com', 'Charlie Brown', 'tenant-2', false);

INSERT INTO orders (user_id, tenant_id, total_amount, status) VALUES
    (1, 'tenant-1', 99.99, 'completed'),
    (1, 'tenant-1', 149.50, 'pending'),
    (2, 'tenant-1', 75.00, 'completed'),
    (3, 'tenant-2', 200.00, 'cancelled');
