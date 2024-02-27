import json
import sched, time

import click
from collections import namedtuple

UnprocessedNode = namedtuple('UnprocessedNode', ['run_time', 'node_id'])
Node = namedtuple('Node', ['run_time', 'node_ids'])

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
    dag_file = open(dag_json)
    dag_nodes = json.load(dag_file)
    # First we process the DAG json to determine when each Node ID should be printed
    flattened_dag_file = preprocess_json_file(dag_nodes)
    scheduler = sched.scheduler(time.time, time.sleep)
    # Then we use Python's event scheduler to create a schedule for when to print each node
    create_schedule(flattened_dag_file, scheduler)
    scheduler.run()

def create_schedule(flattened_dag, scheduler):
    def print_node_and_time(node_str):
        print(f'Node(s): {node_str}, Timestamp: {time.time()}')
    for node in flattened_dag:
        node_ids_str = ", ".join(node.node_ids)
        scheduler.enter(node.run_time, 1, print_node_and_time, (node_ids_str,))

def obtain_next_nodes(node_id, dag_nodes, offset=0):
    edges = dag_nodes[node_id]["edges"]
    if edges:
        return [UnprocessedNode(edge + offset, node_id) for node_id, edge in edges.items()]
    else:
        return []

def place_node_in_flattened_dag(flattened_dag, new_node, index=None):
    if index is None:
        # start at the end of the list
        index = len(flattened_dag) - 1
    current_comparison_node = flattened_dag[index]
    if current_comparison_node.run_time < new_node.run_time:
        flattened_dag.insert(index + 1, Node(new_node.run_time, [new_node.node_id]))
    elif current_comparison_node.run_time > new_node.run_time:
        #
        place_node_in_flattened_dag(flattened_dag, new_node, index=index-1)
    else:
        current_comparison_node.node_ids.append(new_node.node_id)

def preprocess_json_file(dag_nodes):
    flattened_dag = []
    start_node_id = None
    for node_id, node_data in dag_nodes.items():
        if dag_nodes[node_id].get("start"):
            start_node_id = node_id
    flattened_dag.append(Node(0, [start_node_id]))
    nodes_to_process = obtain_next_nodes(start_node_id, dag_nodes, offset=0)
    while len(nodes_to_process) > 0:
        # remove the first item from nodes_to_process list
        new_node = nodes_to_process.pop(0)
        # place the current node in the flattened_dag
        place_node_in_flattened_dag(flattened_dag, new_node)
        # Add the next set of nodes in "edges" to nodes_to_process
        nodes_to_process.extend(obtain_next_nodes(new_node.node_id, dag_nodes, offset=new_node.run_time))
    print(flattened_dag)
    return flattened_dag


if __name__ == "__main__":
    main()
