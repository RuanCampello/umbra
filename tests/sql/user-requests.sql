CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50),
    role "admin" | "developer" | "manager" | "intern",
    account_status "active" | "inactive" | "suspended"
);

CREATE TABLE requests (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    title VARCHAR(100),
    priority "low" | "medium" | "high" | "critical",
    category "ui_ux" | "backend" | "database" | "security",
    is_resolved BOOLEAN
);

INSERT INTO users (username, role, account_status) VALUES 
    ('alice_dev', 'developer', 'active'),
    ('bob_manager', 'manager', 'active'),
    ('charlie_root', 'admin', 'active'),
    ('dave_intern', 'intern', 'inactive'),
    ('eve_hacker', 'developer', 'suspended'),
    ('frank_ops', 'admin', 'active'),
    ('grace_ui', 'developer', 'active'),
    ('heidi_qa', 'intern', 'active'),
    ('ivan_db', 'developer', 'active'),
    ('judy_lead', 'manager', 'active');

INSERT INTO requests (user_id, title, priority, category, is_resolved) VALUES 
    (1, 'Fix navbar alignment', 'low', 'ui_ux', false),
    (1, 'Button click lag', 'medium', 'ui_ux', true),
    (3, 'Update SSL certs', 'critical', 'security', true),
    (9, 'Optimize index scans', 'high', 'database', false),
    (2, 'Q3 Report Export', 'medium', 'backend', true),
    (4, 'Fix typo in footer', 'low', 'ui_ux', true),
    (6, 'Rotate API keys', 'critical', 'security', false),
    (1, 'Dark mode text contrast', 'medium', 'ui_ux', false),
    (7, 'Add rust-analyzer support', 'high', 'backend', false),
    (9, 'Migrate to Umbra SQL', 'critical', 'database', false),
    (5, 'Attempted SQL injection logs', 'high', 'security', true),
    (8, 'Login page 404', 'high', 'backend', true),
    (2, 'Dashboard loading slow', 'medium', 'database', false),
    (10, 'Hire more Rust devs', 'critical', 'backend', false),
    (3, 'Audit admin logs', 'medium', 'security', true),
    (1, 'Fix CSS grid', 'low', 'ui_ux', true),
    (9, 'Repair corrupt WAL', 'critical', 'database', true),
    (7, 'Memory leak in parser', 'high', 'backend', false),
    (4, 'Update copyright year', 'low', 'ui_ux', false),
    (2, 'Add CSV export', 'medium', 'backend', true);
