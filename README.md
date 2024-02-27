# DAG Task Runner

A workflow runner script that accepts a DAG represented in JSON and outputs the letter of each vertex after the prescribed elapsed time defined by the edges.

My solution's code can be found in the scripts/dag_task_runner.py file.

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
Node: A, Timestamp: 1709063290.889846
Node: B, Timestamp: 1709063295.8946261
Node: C, Timestamp: 1709063297.8947148
Node: E, Timestamp: 1709063297.8948328
Node: F, Timestamp: 1709063298.899458
Node: G, Timestamp: 1709063307.894979
Node: D, Timestamp: 1709063307.895016
...
```

## Run tests

To run tests, simply run `pytest` in the root directory

## Notes
There's an additional task runner (scripts/dag_task_runner_2.py) which was my initial solution. In this script, I first flattened the data into a sorted list and then made the task schedule. I left it in for now since I thought it might be interesting to discuss the pros & cons of that approach.

## Assumptions

The task runner assumes that the json files are in the following format and that they are true DAGs without cycles.

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