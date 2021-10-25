# Examples

Example prefect pipelines.

## Local Installation
`pip3 install -r requirements.txt`

## Local Run 
`python3 [EXAMPLE_NAME].py`

## Registering to Hosted Prefect Server:
1. Comment out/remove `state = flow.run()` in `prefect_bgraph_pipe.py`.
2. Uncomment line containing `flow.register(project_name="examples")` in `[EXAMPLE_NAME].py`.
3. Secure copy (scp) `prefect_bgraph_pipe.py` file to hosted prefect server. scp required .env files
4. Ensure hosted prefect server has all requirements in `requirements.txt` installed.
5. Run flow on hosted prefect server console with `python [EXAMPLE_NAME].py` to register. See: https://docs.prefect.io/orchestration/tutorial/first.html#register-a-flow
