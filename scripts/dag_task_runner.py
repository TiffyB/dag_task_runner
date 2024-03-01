import sys
import json
import click
import sched, time

from collections import namedtuple

Task = namedtuple('Task', ['run_time', 'node_id'])


@click.command()
@click.option("--dag_json", default="data/dag_example.json")
def main(dag_json: str):
    """
    Creates a task runner that runs a DAG JSON file and prints the output of vertex

    Inputs:
    --dag_json: a DAG in JSON format. Example format:
        {
            "A": {
                "start": true,
                "edges": {"B": 5, "C": 7}
            },
            "B": {
                "edges": {}
            },
            "C": {
                "edges": {}
            }
        }
    """
    scheduler = sched.scheduler(time.monotonic, time.sleep)
    dag_file = open(dag_json)
    dag_json_obj = json.load(dag_file)
    for node_id, node_data in dag_json_obj.items():
        if dag_json_obj[node_id].get("start"):
            start_node_id = node_id
    start_time = time.monotonic()
    create_task_runner({start_node_id: 0}, dag_json_obj, scheduler, start_time)
    scheduler.run()

def print_node_and_time(node_id):
    print(f'Node: {node_id}, Timestamp: {time.time()}', file=sys.stdout)

def create_task_runner(tasks, dag_json_obj, scheduler, start_time):
    for node_id, task_run_time in tasks.items():
        scheduler.enterabs(start_time + task_run_time, 1, print_node_and_time, (node_id,))
        child_tasks = dag_json_obj[node_id]["edges"]
        create_task_runner(child_tasks, dag_json_obj, scheduler, start_time + task_run_time)
    return scheduler.queue


if __name__ == "__main__":
    main()
