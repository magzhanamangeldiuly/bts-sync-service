-- ============================================
-- Oracle DDL для таблиц синхронизации (OB_V2)
-- ============================================

-- Таблица SITES
CREATE TABLE ob_sites_v2 (
    name VARCHAR2(50) PRIMARY KEY,
    rnc VARCHAR2(50),
    bsc VARCHAR2(50),
    latitude NUMBER(10,6),
    longitude NUMBER(10,6),
    operator VARCHAR2(50),
    kato VARCHAR2(50),
    is_test NUMBER(1),
    address VARCHAR2(500),
    source VARCHAR2(50),
    type VARCHAR2(50),
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ob_sites_v2_insert_date ON ob_sites_v2(insert_date);
CREATE INDEX idx_ob_sites_v2_operator ON ob_sites_v2(operator);
CREATE INDEX idx_ob_sites_v2_type ON ob_sites_v2(type);

COMMENT ON TABLE ob_sites_v2 IS 'Таблица сайтов (основные + transport + rollout)';
COMMENT ON COLUMN ob_sites_v2.name IS 'Уникальное имя сайта';
COMMENT ON COLUMN ob_sites_v2.rnc IS 'RNC контроллер (для UMTS/3G)';
COMMENT ON COLUMN ob_sites_v2.bsc IS 'BSC контроллер (для GSM/2G)';
COMMENT ON COLUMN ob_sites_v2.latitude IS 'Широта';
COMMENT ON COLUMN ob_sites_v2.longitude IS 'Долгота';
COMMENT ON COLUMN ob_sites_v2.operator IS 'Оператор (MTS, KCELL, BEELINE)';
COMMENT ON COLUMN ob_sites_v2.kato IS 'Код КАТО (территориальная принадлежность)';
COMMENT ON COLUMN ob_sites_v2.is_test IS 'Тестовый сайт (1 - да, 0 - нет)';
COMMENT ON COLUMN ob_sites_v2.address IS 'Адрес сайта';
COMMENT ON COLUMN ob_sites_v2.source IS 'Источник данных (RADIO_DATA, KCELL_CM, 250_PLUS, ATOLL, CIPTRACKER)';
COMMENT ON COLUMN ob_sites_v2.type IS 'Тип сайта (main, transport, rollout)';
COMMENT ON COLUMN ob_sites_v2.insert_date IS 'Дата последнего обновления';

---------------------------------------------------

-- Таблица CELLS
CREATE TABLE ob_cells_v2 (
    cell VARCHAR2(100) PRIMARY KEY,
    site VARCHAR2(50),
    sector VARCHAR2(10),
    cellid NUMBER(19),
    lac NUMBER,
    type VARCHAR2(20),
    status VARCHAR2(50),
    band VARCHAR2(50),
    azimut NUMBER,
    height NUMBER,
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ob_cells_v2_site ON ob_cells_v2(site);
CREATE INDEX idx_ob_cells_v2_type ON ob_cells_v2(type);
CREATE INDEX idx_ob_cells_v2_status ON ob_cells_v2(status);
CREATE INDEX idx_ob_cells_v2_insert_date ON ob_cells_v2(insert_date);

COMMENT ON TABLE ob_cells_v2 IS 'Таблица ячеек (GSM/UMTS/LTE/NR)';
COMMENT ON COLUMN ob_cells_v2.cell IS 'Уникальное имя ячейки';
COMMENT ON COLUMN ob_cells_v2.site IS 'Имя сайта';
COMMENT ON COLUMN ob_cells_v2.sector IS 'Сектор (A, B, C, D)';
COMMENT ON COLUMN ob_cells_v2.cellid IS 'Cell ID';
COMMENT ON COLUMN ob_cells_v2.lac IS 'LAC/TAC';
COMMENT ON COLUMN ob_cells_v2.type IS 'Технология (GSM, UMTS, LTE, NR)';
COMMENT ON COLUMN ob_cells_v2.status IS 'Статус (ACTIVATED, DEACTIVATED)';
COMMENT ON COLUMN ob_cells_v2.band IS 'Частотный диапазон (900, 1800, 2100, и т.д.)';
COMMENT ON COLUMN ob_cells_v2.azimut IS 'Азимут антенны (градусы)';
COMMENT ON COLUMN ob_cells_v2.height IS 'Высота антенны (метры)';
COMMENT ON COLUMN ob_cells_v2.insert_date IS 'Дата последнего обновления';

---------------------------------------------------

-- Таблица INTERFERENCES
CREATE TABLE ob_interferences_v2 (
    cell VARCHAR2(100) PRIMARY KEY,
    value NUMBER(19),
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ob_interferences_v2_insert_date ON ob_interferences_v2(insert_date);

COMMENT ON TABLE ob_interferences_v2 IS 'Таблица интерференций ячеек';
COMMENT ON COLUMN ob_interferences_v2.cell IS 'Имя ячейки';
COMMENT ON COLUMN ob_interferences_v2.value IS 'Значение интерференции';
COMMENT ON COLUMN ob_interferences_v2.insert_date IS 'Дата последнего обновления';

---------------------------------------------------

-- Таблица WORKS
CREATE TABLE ob_works_v2 (
    site VARCHAR2(50),
    work_type VARCHAR2(100),
    status VARCHAR2(50),
    insert_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_ob_works_v2 PRIMARY KEY (site, work_type, insert_date)
);

CREATE INDEX idx_ob_works_v2_status ON ob_works_v2(status);
CREATE INDEX idx_ob_works_v2_insert_date ON ob_works_v2(insert_date);

COMMENT ON TABLE ob_works_v2 IS 'Таблица работ на сайтах (из Ciptracker)';
COMMENT ON COLUMN ob_works_v2.site IS 'Имя сайта';
COMMENT ON COLUMN ob_works_v2.work_type IS 'Тип работы';
COMMENT ON COLUMN ob_works_v2.status IS 'Статус работы';
COMMENT ON COLUMN ob_works_v2.insert_date IS 'Дата обновления';

---------------------------------------------------

-- Представления для удобства работы с актуальными данными

-- Актуальные сайты (последняя синхронизация)
CREATE OR REPLACE VIEW v_ob_sites_v2_latest AS
SELECT *
FROM ob_sites_v2
WHERE insert_date = (SELECT MAX(insert_date) FROM ob_sites_v2);

-- Актуальные ячейки (последняя синхронизация)
CREATE OR REPLACE VIEW v_ob_cells_v2_latest AS
SELECT *
FROM ob_cells_v2
WHERE insert_date = (SELECT MAX(insert_date) FROM ob_cells_v2);

-- Актуальные интерференции (последняя синхронизация)
CREATE OR REPLACE VIEW v_ob_interferences_v2_latest AS
SELECT *
FROM ob_interferences_v2
WHERE insert_date = (SELECT MAX(insert_date) FROM ob_interferences_v2);

-- Актуальные работы (последняя синхронизация)
CREATE OR REPLACE VIEW v_ob_works_v2_latest AS
SELECT *
FROM ob_works_v2
WHERE insert_date = (SELECT MAX(insert_date) FROM ob_works_v2);

-- Статистика по сайтам
CREATE OR REPLACE VIEW v_ob_sites_v2_stats AS
SELECT 
    operator,
    type,
    COUNT(*) as total_sites,
    COUNT(CASE WHEN is_test = 1 THEN 1 END) as test_sites,
    COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as sites_with_coords
FROM v_ob_sites_v2_latest
GROUP BY operator, type;

-- Статистика по ячейкам
CREATE OR REPLACE VIEW v_ob_cells_v2_stats AS
SELECT 
    type,
    status,
    band,
    COUNT(*) as total_cells,
    COUNT(CASE WHEN azimut IS NOT NULL THEN 1 END) as cells_with_azimut,
    COUNT(CASE WHEN height IS NOT NULL THEN 1 END) as cells_with_height
FROM v_ob_cells_v2_latest
GROUP BY type, status, band;

-- Полная информация: сайты + ячейки + интерференции
CREATE OR REPLACE VIEW v_ob_full_cell_info_v2 AS
SELECT 
    c.*,
    s.latitude as site_latitude,
    s.longitude as site_longitude,
    s.operator,
    s.kato,
    s.address as site_address,
    s.type as site_type,
    i.value as interference_value
FROM v_ob_cells_v2_latest c
LEFT JOIN v_ob_sites_v2_latest s ON c.site = s.name
LEFT JOIN v_ob_interferences_v2_latest i ON c.cell = i.cell;

COMMIT;


