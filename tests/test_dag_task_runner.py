from click.testing import CliRunner
from scripts.dag_task_runner import main

def create_dict_from_stdout(result):
    result_lines = result.output.split('\n')
    result_dict = {}
    for line in result_lines:
        # make sure it isn't the empty line at the end of the output
        if line:
            node, timestamp = line.split(", Timestamp: ")
            node_id = node.split(": ")[1]
            if node_id in result_dict:
                result_dict[node_id].append(float(timestamp))
            else:
                result_dict[node_id] = [float(timestamp)]
    print(result_dict)
    return result_dict

def test_dag_task_runner():
    runner = CliRunner()
    result = runner.invoke(main, ['--dag_json', 'data/dag_example.json'])
    result_dict = create_dict_from_stdout(result)
    assert round(result_dict['B'][0] - result_dict['A'][0], 1) == 5
    assert round(result_dict['C'][0] - result_dict['A'][0], 1) == 7
    assert round(result_dict['G'][0] - result_dict['A'][0], 1) == 17
    assert round(result_dict['E'][0] - result_dict['B'][0], 1) == 2
    assert round(result_dict['D'][0] - result_dict['C'][0], 1) == 10
    assert round(result_dict['F'][0] - result_dict['E'][0], 1) == 1

def test_combo_long_and_short_running_vertices():
    runner = CliRunner()
    result = runner.invoke(main, ['--dag_json', 'data/dag_example_2.json'])
    result_dict = create_dict_from_stdout(result)
    # ensure long-running vertex executes at the correct time relative to start node
    assert round(result_dict['G'][0] - result_dict['A'][0], 1) == 30
    assert round(result_dict['H'][0] - result_dict['G'][0], 1) == 30
    # ensure short-running vertex also executes at the correct time relative to the start node
    assert round(result_dict['B'][0] - result_dict['A'][0], 1) == 2
    assert round(result_dict['E'][0] - result_dict['B'][0], 1) == 2
    assert round(result_dict['F'][0] - result_dict['E'][0], 1) == 1
    assert round(result_dict['I'][0] - result_dict['F'][0], 1) == 1
    assert round(result_dict['J'][0] - result_dict['I'][0], 1) == 1
    assert round(result_dict['K'][0] - result_dict['J'][0], 1) == 1

def test_vertices_with_multiple_parents():
    '''
    Here we want to ensure that a DAG that has a non-starting node with multiple parents, such as node D in the example
    below, will print D twice (once at 8 seconds elapsed, and again at 10 seconds elapsed):

    A--(5)--B--(3)--D
     \             /
      -(7)--C--(3)-
    '''
    runner = CliRunner()
    result = runner.invoke(main, ['--dag_json', 'data/multiple_parent_nodes.json'])
    result_dict = create_dict_from_stdout(result)
    assert round(result_dict['D'][0] - result_dict['A'][0], 1) == 8
    assert round(result_dict['D'][1] - result_dict['A'][0], 1) == 10