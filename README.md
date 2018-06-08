# ETL Framework

This is an overly complicated Extract-Transform-Load Framework for working with spreadsheeds and databases. Currently, it only supports `.xls` and `.xlsx` files. The files can also be extracted from archives (currently only `.rar` and `.zip` supported).

## Getting Started

The main entry point to the ETL is `run.py`, which loads jobs contained in `/jobs` queue.

```bash
$ venv/bin/python run.py
```

`run.py` launches `crawler.crawler.run()` process that loads all jobs contained in `/jobs`. These are either `.json` or `.ini` files of the following format:

```ini
[WHO_STAT_C_MORTALITY]
last_row = [None]
store = full\path\to\data\WHO_STAT_MORTALITY
urls = ['http://www.who.int/healthinfo/statistics/whostat2005_mortality.xls']
path = []
index_col = Num
structure = ['Num', 'Country', 'WHO_Region', 'life_exp_m', 'life_exp_f', 'h_life_exp_m', 'h_life_exp_f', 'P_death_m', 'P_death_f', 'P_death_5y', 'P_death_28d', 'R_death_maternal']
skip_row = [7]
sheet = [None]
```

_Here, we fetch WHO mortality statistics._

As you can guess, such a configuration relies on stability of the source format (i.e. its' structure does not change over long periods of time). If you are dealing with sources where the URL changes, but you can still generate it from the site's HTML info, you can create a script that returns the above information as a `dict` object and place it in the jobspecs folder, say `jobspecs/job.py`. To queue:

```bash
$ venv/bin/python update_jobs.py 
```

Once there, you can run the `update_jobs.py` script to update the `/jobs` folder with the correct job models.

### Prerequisites

At least Python 3.5 is needed.

Due to licensing, some third party libraries are needed:

- [Oracle Client](https://oracle.github.io/odpi/doc/installation.html#linux) libraries
- [UnRAR](http://rarfile.readthedocs.io/en/latest/faq.html#what-are-the-dependencies) libraries

### Installing

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

On Ubuntu, you can schedule a timer using `systemd`

```bash

```
## Built With

* [pandas](https://pandas.pydata.org/) - Python Data Analysis Library
* [cx_Oracle](https://oracle.github.io/python-cx_Oracle/) - Python extension module that enables access to Oracle Database

## Authors

* **Serzhan Akhmetov** - *Initial work* - [siaarzh](https://github.com/siaarzh)

## License

TBD

## Acknowledgments

* [elessarelfstone](https://github.com/elessarelfstone) - Implemented his Oracle connection method
