# BTS Sync Service

Сервис синхронизации данных базовых станций (BTS) из различных источников в единую PostgreSQL базу данных.

## Архитектура

Сервис использует Spring Integration для организации потоков синхронизации данных.

### Источники данных

- **PostgreSQL**: radio_data_all, kcell_cm, beeline_kcell_250_plus
- **Atoll (SQL Server)**: данные о сайтах, азимутах и высотах антенн
- **Atoll Transport (SQL Server)**: транспортные сайты
- **Ciptracker (MySQL)**: rollout сайты и работы на сайтах
- **MariaDB**: данные об интерференциях ячеек

### Целевая база данных

PostgreSQL с 4 таблицами:
- `sites` - информация о сайтах (включая lat/lon, kato, address)
- `cells` - информация о ячейках (включая azimut и height)
- `interferences` - интерференции ячеек
- `works` - работы на сайтах

## Потоки синхронизации

### 1. Sites (основные сайты)
Объединяет данные из 3 источников (RADIO_DATA + KCELL_CM + 250_PLUS), обогащает данными из Atoll (lat/lon/kato/address).

### 2. Transport Sites
Синхронизирует транспортные сайты, которых нет в основных источниках.

### 3. Rollout Sites
Синхронизирует rollout сайты, которых нет в основных источниках.

### 4. Cells
Синхронизирует ячейки только для существующих сайтов, обогащает данными azimut и height из Atoll.

### 5. Site Works
Синхронизирует работы только для существующих сайтов.

### 6. Cell Interference
Синхронизирует интерференции только для существующих ячеек.

## Последовательность выполнения

Синхронизация запускается каждый час в следующей последовательности:

1. Sites (main) → Transport Sites → Rollout Sites
2. Cells + Site Works (параллельно)
3. Cell Interference

## Настройка

### Переменные окружения

```properties
RANSHARING_DB_URL=localhost:5432/ransharing
RANSHARING_DB_USERNAME=postgres
RANSHARING_DB_PASSWORD=password

ALARMS_DB_URL=localhost:1521/alarms
ALARMS_DB_USERNAME=oracle_user
ALARMS_DB_PASSWORD=password

MARIA_DB_URL=localhost:3306/maria
MARIA_DB_USERNAME=maria_user
MARIA_DB_PASSWORD=password

ATOLL_DB_URL=localhost:1433;databaseName=atoll
ATOLL_DB_USERNAME=atoll_user
ATOLL_DB_PASSWORD=password

ATOLL_TRANSPORT_DB_URL_ENCRYPT=localhost:1433;databaseName=atoll_transport;encrypt=false
CIPTRACKER_DB_URL=localhost:3306/ciptracker
CIPTRACKER_DB_USERNAME=ciptracker_user
CIPTRACKER_DB_PASSWORD=password
```

### Создание таблиц

Выполните SQL скрипт `src/main/resources/schema.sql` в целевой PostgreSQL базе данных.

## Запуск

```bash
mvn spring-boot:run
```

## Особенности реализации

- Все данные обрабатываются как `Map<String, Object>` (без использования records)
- Используется `insert_date` для версионирования данных
- Все SQL запросы фильтруют данные по `where insert_date = (select max(insert_date) ...)`
- Azimut и height хранятся непосредственно в таблице cells
- Нет delete flows - только upsert операции
- Batch операции для эффективной вставки данных
