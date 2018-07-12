# ETL Framework

This is an overly complicated Extract-Transform-Load Framework for working with spreadsheeds and databases. Currently, it only supports `.xls` and `.xlsx` files. The files can also be extracted from archives (currently only `.rar` and `.zip` supported).

## Getting Started

The main entry point to the ETL is `run.py`, which loads jobs contained in `/jobs` queue.

```bash
$ venv/bin/python run.py
```

`run.py` launches `crawler.crawler.run()` process that loads all jobs contained in `/jobs`. These are either `.json` or `.ini` parameter files.

```ini
[TABLE]
name1 = value1
name2 = value2
...
```
Every section of an `ini` file corresponds to the table defined in the destination database with the following parameters:

| Parameter   | Format           | Description                                                                                                                                                      |
| ----------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| *urls*      | List of strings  | List of urls of the source material. These can be spreadsheets or archives of `xls`s of identical structure. E.g.: `['url1', 'url2']`                            |
| *structure* | List of strings  | Names of table columns.                                                                                                                                          |
| *store*     | String           | A full path to the temporary storage directory for spreadsheets and archives                                                                                     |
| *index_col* | String           | Name of the column with unique uninterrupted names or IDs in the spreadsheed. This is used for filtering out blank lines at the end of sheets                    |
| *sheet*     | List of integers | Select specific sheets if necessary. Zero-based numbering means sheet #1 and #3 means `[0,2]`. A value of `[None]` means include all sheets                      |
| *skip_row*  | List of integers | For every source url, select how many rows to skip before starting to read data. If your data starts at cell 4 in `url1` and cell 1 in `url2`, use `[3, 0]`.     |
| *last_row*  | List of integers | For every source url, select the last row to read. If your data ends starts at cell 100 in `url1` and cell 200 in `url2`, use `[100, 200]`. **NOT IMPLEMENTED!** |
| *path*      | Blank List       | **Placeholder for crawler**. Always set to `[]`                                                                                                                  |

### Example:
Here is an example where we fetch WHO mortality statistics:

Using `ini` format:
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
Or `json` format:
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
As you can guess, such a configuration relies on stability of the source format (i.e. its' structure does not change over long periods of time). If you are dealing with sources where the URL changes, but you can still generate it from the site's HTML info, you can create a script that returns the above information as a `dict` object and place it in the jobspecs folder, say `jobspecs/job.py`. To queue:

```bash
$ venv/bin/python update_jobs.py 
```

Once there, you can run the `update_jobs.py` script to update the `/jobs` folder with the correct job models. Currently, the output format is only `ini`.

## Prerequisites

At least Python 3.5 is needed.

Due to licensing, some third party libraries are needed:

- [Oracle Client](https://oracle.github.io/odpi/doc/installation.html#linux) libraries
- [UnRAR](http://rarfile.readthedocs.io/en/latest/faq.html#what-are-the-dependencies) libraries

## Installing

1. Download or `clone` the repository
2. Install the the necessary packages:

```bash
~ $ git clone git@bitbucket.org:siaarzh/telecom_crawler.git 
~ $ cd telecom_crawler
telecom_crawler $ pip install -r requirements.txt
```

## Running the tests

TBD

## Deployment

On Linux, you can schedule a timer using `systemd`. Create a `.service` and `.timer` with the same name in the `/etc/systemd/system/` directory. 

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
---
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
Now [start](https://wiki.archlinux.org/index.php/Systemd#Using_units) and [load](https://wiki.archlinux.org/index.php/Systemd#Using_units) the timer with the following commands:
```bash
$ sudo systemctl start etl.timer
$ sudo systemctl enable etl.timer
```
More info on systemd timers can be found at the [official documentation](https://wiki.archlinux.org/index.php/Systemd/Timers)
## Built With

* [pandas](https://pandas.pydata.org/) - Python Data Analysis Library
* [cx_Oracle](https://oracle.github.io/python-cx_Oracle/) - Python extension module that enables access to Oracle Database

## Authors

* **Serzhan Akhmetov** - *Initial work* - [siaarzh](https://github.com/siaarzh)

## License

TBD

## Acknowledgments

* [elessarelfstone](https://github.com/elessarelfstone) - Implemented his Oracle connection method
