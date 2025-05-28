CREATE TABLE IF NOT EXISTS pubs (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    is_haunted BOOLEAN DEFAULT FALSE,
    last_inspection DATE
);

CREATE TABLE IF NOT EXISTS patrons (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    tab INTEGER,  -- Using INTEGER for money (because we're not monsters)
    is_rowdy BOOLEAN DEFAULT TRUE,
    pub_id INTEGER
);

INSERT INTO pubs VALUES
    (1, 'The Cloaked Bat',       TRUE,  '2023-11-01'),
    (2, 'The Rusty Spoon',       FALSE, '2023-10-15'),
    (3, 'The Overflowing Pint',  TRUE,  NULL),
    (4, 'The Drowned Rat',       TRUE,  '2023-09-30'),
    (5, 'The Tipsy Vicar',       FALSE, '2023-11-20'),
    (6, 'The Binary Alehouse',   FALSE, NULL),
    (7, 'The Phantom Piper',     TRUE,  '2023-12-25')
ON CONFLICT DO NOTHING;

INSERT INTO patrons VALUES
    (1,  'Reginald',   42,   FALSE, 1),
    (2,  'Mildred',    87,   TRUE,  1),
    (3,  'The Vicar',  NULL, FALSE, 1),
    (4,  'Brenda',     500,  TRUE,  2),
    (5,  'Gary',       12,   TRUE,  2),
    (6,  'Clive',      0,    FALSE, 2),
    (7,  'Steve',      0,    TRUE,  3),
    (8,  'Dave',       35,   TRUE,  3),
    (9,  'Ron',        -10,  TRUE,  4),
    (10, 'Sue',        NULL, FALSE, 4),
    (11, 'Geoffrey',   22,   FALSE, 5),
    (12, 'Margaret',   150,  FALSE, 5),
    (13, 'Terry',      11,   FALSE, 6),
    (14, 'Alice',      101,  FALSE, 6),
    (15, 'Phantom Ed', NULL, FALSE, 7),
    (16, 'Living Liz', 5,    TRUE,  7)
ON CONFLICT DO NOTHING;
