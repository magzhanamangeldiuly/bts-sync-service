CREATE TABLE IF NOT EXISTS sites (
    name VARCHAR(50) PRIMARY KEY,
    rnc VARCHAR(50),
    bsc VARCHAR(50),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    operator VARCHAR(50),
    kato VARCHAR(50),
    is_test INTEGER,
    address VARCHAR(500),
    source VARCHAR(50),
    type VARCHAR(50),
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sites_insert_date ON sites(insert_date);
CREATE INDEX IF NOT EXISTS idx_sites_operator ON sites(operator);
CREATE INDEX IF NOT EXISTS idx_sites_type ON sites(type);

CREATE TABLE IF NOT EXISTS cells (
    cell VARCHAR(100) PRIMARY KEY,
    site VARCHAR(50),
    sector VARCHAR(10),
    cellid BIGINT,
    lac INTEGER,
    type VARCHAR(20),
    status VARCHAR(50),
    band VARCHAR(50),
    azimut INTEGER,
    height INTEGER,
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (site) REFERENCES sites(name)
);

CREATE INDEX IF NOT EXISTS idx_cells_site ON cells(site);
CREATE INDEX IF NOT EXISTS idx_cells_insert_date ON cells(insert_date);
CREATE INDEX IF NOT EXISTS idx_cells_type ON cells(type);
CREATE INDEX IF NOT EXISTS idx_cells_status ON cells(status);

CREATE TABLE IF NOT EXISTS interferences (
    cell VARCHAR(100) PRIMARY KEY,
    value BIGINT,
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (cell) REFERENCES cells(cell)
);

CREATE INDEX IF NOT EXISTS idx_interferences_insert_date ON interferences(insert_date);

CREATE TABLE IF NOT EXISTS works (
    site VARCHAR(50),
    work_type VARCHAR(100),
    status VARCHAR(50),
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (site, work_type),
    FOREIGN KEY (site) REFERENCES sites(name)
);

CREATE INDEX IF NOT EXISTS idx_works_insert_date ON works(insert_date);
CREATE INDEX IF NOT EXISTS idx_works_status ON works(status);

