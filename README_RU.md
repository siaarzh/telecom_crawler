(перевод: google translate)

# ETL Framework

Это чрезмерно сложный фреймворк Extract-Transform-Load для работы с excel таблицами и базами данных. В настоящее время он поддерживает файлы `.xls` и `.xlsx`. Файлы также могут быть извлечены из архивов (в настоящее время поддерживаются только `.rar` и `.zip`).

## С чего начать

Основной точкой входа в ETL является `run.py`, которая загружает поочерёдно задания, содержащиеся в папке `/jobs`.

```bash
$ venv/bin/python run.py
```

`run.py` запускает процесс `crawler.crawler.run()`, который загружает все задания, содержащиеся в `/jobs`. Заданиями являются конфиг-файлы в формате `.json` или `.ini`. Проше всего использовать `.ini`:

```ini
[TABLE]
name1 = value1
name2 = value2
...
```
Каждый раздел файла `ini` соответствует таблице  (к примеру `[TABLE]`), определенной в базе данных назначения, и заполнен следующими параметрами:

| Параметр    | Формат           | Описание                                                                                                                                                                                                 |
| ----------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| *urls*      | Кортеж `str`     | Список ссылок материалов источника. Это могут быть excel таблицы или архивы с кучей `xls`-файлов идентичной структуры. Например: `['url1', 'url2']`                                                      |
| *structure* | Кортеж `str`     | Имена столбцов таблицы.                                                                                                                                                                                  |
| *store*     | `str`            | Полный путь к папке временного хранения для таблиц и архивов                                                                                                                                             |
| *index_col* | `str`            | Имя столбца содержащего уникальные непустые сначения или идентификаторы в таблице. Это используется для фильтрации пустых строк в конце листов                                                           |
| *sheet*     | Кортеж `int`     | При необходимости выберите конкретные листы. Нумерация с нуля, т.е. листы №1 и №3 означают `[0,2]`. Значение `[None]` означает все листы                                                                 |
| *skip_row*  | Кортеж `int`     | Для каждого исходного URL выберите количество пропущенных строк перед началом чтения данных. Если ваши данные начинаются в ячейках 4-го рядя в `url1` и ячейках 1-го ряда в `url2`, используйте `[3, 0]`.|
| *last_row*  | Кортеж `int`     | Для каждого исходного URL-адреса выберите последнюю строку для чтения. Если ваши данные заканчиваются в ячейке 100 в `url1` и ячейке 200 в `url2`, используйте `[100, 200]`. ** НЕ ВНЕДРЕННО! **         |
| *path*      | Кортеж           | ** ОСТАВИТЬ ПУСТЫМ **. `[]`                                                                                                                                                                              |

### Example:
Вот пример, где мы получаем статистику смертности ВОЗ (Всемирная Организация Здравохранения):

Используя формат `ini`:
```ini
[WHO_STAT_MORTALITY]
urls = ['http://www.who.int/healthinfo/statistics/whostat2005_mortality.xls']
structure = ['Num', 'Country', 'WHO_Region', 'life_exp_m', 'life_exp_f', 'h_life_exp_m', 'h_life_exp_f', 'P_death_m', 'P_death_f', 'P_death_5y', 'P_death_28d', 'R_death_maternal']
store = full/path/to/data/WHO_STAT_MORTALITY
index_col = Num
sheet = [None]
skip_row = [7]
last_row = [None]
path = []
```
Используя формат `json`:
```json
{
  "WHO_STAT_MORTALITY": {
    "index_col": "Num",
    "path": [],
    "store": "full/path/to/data/WHO_STAT_MORTALITY",
    "urls": [
      "http://www.who.int/healthinfo/statistics/whostat2005_mortality.xls"
    ],
    "structure": [
      "Num",
	  "Country",
	  "WHO_Region",
	  "life_exp_m",
	  "life_exp_f",
	  "h_life_exp_m",
	  "h_life_exp_f",
	  "P_death_m",
	  "P_death_f",
	  "P_death_5y",
	  "P_death_28d",
	  "R_death_maternal"
    ],
    "skip_row": [
      7
    ],
    "last_row": [
      null
    ],
    "sheet": [
      null
    ]
  }
}
```
Как можно догадаться, такая конфигурация зависит от стабильности исходного формата (т.e. структура не должна меняется в течение длительных периодов времени). Если вы имеете дело с источниками, где URL-адрес изменяется, но вы все равно можете генерировать его из  HTML кода страницы, вы можете создать python-скрипт, который возвращает выше указанную информацию как объект `dict` и поместить ее в папку jobspecs, например `jobspecs/job.py`.

После этого вы можете запустить скрипт `update_jobs.py`, чтобы заполнить папку `/jobs` очередью заданий. В настоящее время код выдает только `ini` файлы.

```bash
$ venv/bin/python update_jobs.py 
```

## Tребования

Требуется, по крайней мере, Python 3.5.

Из-за лицензирования необходимы некоторые сторонние библиотеки:

- Библиотеки [Oracle Client](https://oracle.github.io/odpi/doc/installation.html#linux)
- Библиотеки [UnRAR](http://rarfile.readthedocs.io/en/latest/faq.html#what-are-the-dependencies)

## Установка

1. Загрузить или «клонировать» репозиторий
2. Установите необходимые пакеты:

```bash
~ $ git clone https://github.com/siaarzh/telecom_crawler.git 
~ $ cd telecom_crawler
telecom_crawler $ pip install -r requirements.txt
```

## Выполнение тестов

TBD

## Запуск

В Linux можно создать расписание для периодического запуска с помощью `systemd`. Достаточно создать `.service` и `.timer` с идентичными именами в папке `/etc/systemd/system/`.

```ini
# /etc/systemd/system/etl.service
[Unit]
Description=ETL worker

[Service]
Type=simple
WorkingDirectory=/path/to/workdir
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:path/to/workdir/venv/bin"
ExecStart=path/to/workdir/venv/bin/python run.py
```

```ini
# /etc/systemd/system/etl.timer
[Unit]
Description=Run etl.service every week on Monday

[Timer]
# format is DayOfWeek Year-Month-Day Hour:Minute:Second
OnCalendar=Mon *-*-* 13:00:00

[Install]
WantedBy=timers.target
```
Теперь [стартуй](https://wiki.archlinux.org/index.php/Systemd#Using_units) и [загрузи](https://wiki.archlinux.org/index.php/Systemd#Using_units) таймер:
```bash
$ sudo systemctl start etl.timer
$ sudo systemctl enable etl.timer
```
Более подробную информацию о таймерах systemd можно найти в [официальной документации](https://wiki.archlinux.org/index.php/Systemd/Timers)

## Построено с помошью

* [pandas](https://pandas.pydata.org/) - Python Data Analysis Library
* [cx_Oracle](https://oracle.github.io/python-cx_Oracle/) - Python extension module that enables access to Oracle Database

## Авторы

* **Сержан Ахметов** - *Начало* - [siaarzh](https://github.com/siaarzh)

## Лицензирование

TBD

## Благодарности

* [elessarelfstone](https://github.com/elessarelfstone) - реализовал метод соединения с Oracle
