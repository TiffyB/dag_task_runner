# DAG Task Runner

A workflow runner script that accepts a DAG represented in JSON and outputs the letter of each vertex after the prescribed elapsed time defined by the edges

## Setup

This task runner was built on Python 3.11.2 using [pyenv](https://github.com/pyenv/pyenv). To get set up, run:

```
pyenv install 3.11.2
pyenv local 3.11.2
```

Then download the project requirements by running:

```
pip install -r requirements.txt
```

## Run program

To run the program, use the command below:
```
python scripts/dag_task_runner.py --dag_json data/dag_example.json
```

Once running, you'll see the printout of nodes and the time they were printed at, i.e.:

```
Node: A, Timestamp: 1709062016.0527518
Node: B, Timestamp: 1709062021.0554729
Node: C, Timestamp: 1709062023.056127
Node: D, Timestamp: 1709062024.058493
...
```

## Run tests

To run tests, simply run `pytest` in the root directory

## Assumptions

The task runner assumes that the json files are in the following format and that they are true DAGs without cycles 

```
{
  "A": {
    "start": true,
    "edges": {
      "B": 5,
      "C": 7
    }
  },
  "B": {},
  "C": {},
}
```