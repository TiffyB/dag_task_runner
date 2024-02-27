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
    spawn_task_runner([Task(0, start_node_id)], dag_json_obj, scheduler, start_time)

def prepare_tasks(edges):
    return [Task(edge_value, edge_id) for edge_id, edge_value in edges.items()]

def spawn_task_runner(tasks, dag_json_obj, scheduler, start_time):
    def print_node_and_time(node_id):
        print(f'Node: {node_id}, Timestamp: {time.time()}', file=sys.stdout)
        breadth_first_search(node_id, dag_json_obj, scheduler, time.monotonic())
    for task in tasks:
        scheduler.enterabs(start_time + task.run_time, 1, print_node_and_time, (task.node_id,))
    scheduler.run()

def breadth_first_search(current_node_id, dag_json_obj, scheduler, start_time):
    tasks = prepare_tasks(dag_json_obj[current_node_id]["edges"])
    spawn_task_runner(tasks, dag_json_obj, scheduler, start_time)


if __name__ == "__main__":
    main()
