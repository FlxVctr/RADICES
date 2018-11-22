# SparseTwitter
Project to create a sparsified sample network of (German)Twitter Users


## Setup dev environment

Install [pipenv](https://pipenv.readthedocs.io/en/latest/) and run:

```
pipenv install --dev
```

Run

```
pipenv shell
```

to start shell in virtual env.

## Testing

To avoid `ModuleNotFoundError`s, the PYTHONPATH needs to be set (especially on
  Windows). For Windows, this can be achieved as follows. Open cmd, then type:
```
set PYTHONPATH=%PYTHONPATH%;complete\path\to\project\root
```

The tests directory contains all unittests (`tests.py`) as well as all
  functional tests (`functional_tests.py`). To run all tests at once, run
  `python tests/tests.py` or `python tests/functional_tests.py`. 
