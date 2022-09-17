from datetime import datetime
from paradag import MultiThreadProcessor
from paradag import SequentialProcessor
import time
from paradag import SingleSelector, RandomSelector, ShuffleSelector, FullSelector
from paradag import DAG, dag_run
"""

1 -> [2, 3, 4, 5, 6] -> 7
"""
dag = DAG()
dag.add_vertex(1, 2, 3, 4, 5, 6, 7)
for i in [2, 3, 4, 5, 6]:
    dag.add_edge(1, i)
    dag.add_edge(i, 7)
print('Single selector')
print(dag_run(dag, selector=SingleSelector()))
print('RandomSelector selector')
print(dag_run(dag, selector=RandomSelector()))
print('Suffle selector')
print(dag_run(dag, selector=ShuffleSelector()))
print('Full selector')
print(dag_run(dag, selector=FullSelector()))

"""
Execution Experiment
"""
print('Execution Experiment')


class CustomExecutor:
    def param(self, vertex):
        return vertex

    def execute(self, param):
        time.sleep(1)
        return f'result of {param}'

    def report_start(self, vertices):
        print('Start to run:', vertices)

    def report_running(self, vertices):
        print('Current running:', vertices)

    def report_finish(self, vertices_result):
        for vertex, result in vertices_result:
            print(
                'Finished running {0} with result: {1}'.format(
                    vertex, result))


print('Sequential:')
start = datetime.now()
print(
    dag_run(
        dag,
        selector=FullSelector(),
        executor=CustomExecutor(),
        processor=SequentialProcessor()))
end = datetime.now()
print('* Sequential Execution time:', end - start)
print('Parallel:')
start = datetime.now()
print(
    dag_run(
        dag,
        selector=FullSelector(),
        executor=CustomExecutor(),
        processor=MultiThreadProcessor()))
end = datetime.now()
print('* Parallel Execution time:', end - start)
