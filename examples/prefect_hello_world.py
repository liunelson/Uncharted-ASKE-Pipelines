from prefect import task
from prefect import Flow

@task
def add(x, y=1):
    return x + y

with Flow("My first flow!") as flow:
    first_result = add(1, y=2)
    second_result = add(x=first_result, y=100)

state = flow.run()
## NOTE: Uncomment this line when registering to prefect server and comment flow.run line
# flow.register(project_name="examples")
