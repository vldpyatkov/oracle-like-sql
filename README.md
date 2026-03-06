# oracle-like-sql

Плагин для Apache Ignite, который добавляет Oracle-подобные SQL-функции.

## Добавленные SQL-функции

### `SYSTIMESTAMP`
Возвращает текущую дату и время сервера в формате `TIMESTAMP`.

**Пример:**
```sql
SELECT SYSTIMESTAMP;
```

---

### `REGEXP_COUNT(source, pattern[, start_position])`
Считает количество вхождений регулярного выражения `pattern` в строке `source`.

- Необязательный третий параметр `start_position` задаёт позицию, с которой начинается поиск (нумерация с `1`).

**Примеры:**
```sql
SELECT REGEXP_COUNT('abc123abc456', 'abc'); -- 2
SELECT REGEXP_COUNT('a1b2c3', '[0-9]');     -- 3
SELECT REGEXP_COUNT('abcabcabc', 'abc', 4); -- 2
```

---

### `ADD_MONTHS(date_or_timestamp, months)`
Добавляет (или вычитает) указанное число месяцев к дате/времени.

- Поддерживаются типы `DATE` и `TIMESTAMP`.
- Отрицательное значение `months` вычитает месяцы.
- Если целевой месяц короче исходного дня, используется последний день месяца.

**Примеры:**
```sql
SELECT ADD_MONTHS(DATE '2024-01-31', 1);   -- 2024-02-29
SELECT ADD_MONTHS(TIMESTAMP '2024-03-15 10:00:00', -2); -- 2024-01-15 10:00:00
```


---

### `SUBSTR(source, start[, length])`
Возвращает подстроку из `source`, начиная с позиции `start`.

- Если указан `length`, возвращается не более `length` символов.
- Позиция `start` задаётся как в Oracle (`1` — первый символ).

**Примеры:**
```sql
SELECT SUBSTR('ORACLE', 2, 3); -- RAC
SELECT SUBSTR('ORACLE', 4);    -- CLE
```

## Тестирование
В репозитории добавлены интеграционные тесты для новых функций, проверяющие базовые и граничные случаи.
