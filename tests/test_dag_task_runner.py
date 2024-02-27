from click.testing import CliRunner
from scripts.dag_task_runner import main

def create_dict_from_stdout(result):
    result_lines = result.output.split('\n')
    result_dict = {}
    for line in result_lines:
        # make sure it isn't the empty line at the end of the output
        if line:
            node, timestamp = line.split(", Timestamp: ")
            result_dict[node.split(": ")[1]] = float(timestamp)
    return result_dict

def test_dag_task_runner():
    runner = CliRunner()
    result = runner.invoke(main, ['--dag_json', 'data/dag_example.json'])
    result_dict = create_dict_from_stdout(result)
    assert round(result_dict['B'] - result_dict['A'], 1) == 5
    assert round(result_dict['C'] - result_dict['A'], 1) == 7
    assert round(result_dict['G'] - result_dict['A'], 1) == 17
    assert round(result_dict['E'] - result_dict['B'], 1) == 2
    assert round(result_dict['D'] - result_dict['C'], 1) == 10
    assert round(result_dict['F'] - result_dict['E'], 1) == 1

def test_combo_long_and_short_running_vertices():
    runner = CliRunner()
    result = runner.invoke(main, ['--dag_json', 'data/dag_example_2.json'])
    result_dict = create_dict_from_stdout(result)
    # ensure long-running vertex executes at the correct time relative to start node
    assert round(result_dict['G'] - result_dict['A'], 1) == 30
    assert round(result_dict['H'] - result_dict['G'], 1) == 30
    # ensure short-running vertex also executes at the correct time relative to the start node
    assert round(result_dict['B'] - result_dict['A'], 1) == 2
    assert round(result_dict['E'] - result_dict['B'], 1) == 2
    assert round(result_dict['F'] - result_dict['E'], 1) == 1
    assert round(result_dict['I'] - result_dict['F'], 1) == 1
    assert round(result_dict['J'] - result_dict['I'], 1) == 1
    assert round(result_dict['K'] - result_dict['J'], 1) == 1
