Data generated by actively changing the date/time settings of the host.

In `iceberg.properties` add the following properties:

```
iceberg.unique-table-location=false
iceberg.table-statistics-enabled=false
```

Use `trino` to create the table content

```sql
CREATE TABLE iceberg.tiny.timetravel(data integer);
-- increase the date on the host
INSERT INTO iceberg.tiny.timetravel VALUES 1;
-- increase the date on the host
INSERT INTO iceberg.tiny.timetravel VALUES 2;
-- increase the date on the host
INSERT INTO iceberg.tiny.timetravel VALUES 3;
```