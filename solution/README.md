# DWH Coding Challenge
Entrypoint to the program is `main()` function in `solution/src/process_transactions.py`.
It has a few input arguments such as:
* `--input-{accounts,cards,savings-account}`: path to input directory for a specific dataset
* `--wait-for {N}`: sleep for N seconds at the end of the program

Main logic is in `run()` function of `TransactionProcessor` class.
Input files are read as-is with little conversions (timestamp column is converted to `Timestamp` type).
Next step is to flatten nested structures `data` and `set`.
Then historical datasets are calculated based on series of `create` and `update` operations.
For each column the last non-null value up to `ts` timestamp is taken.

Once we have historical dataset, denormalized joined table can be calculated.
Join base dataframe (accounts) with both cards and saving cards. 
Left join is performed by default as foreign keys to cards/saving cards might be missing.
There might be more than one record matching to a single account record. Deduplication is done in a following way:
the last card/saving card record, that happened not later than account record is chosen.

The last step is to calculate transactions based on `cards_history` and `saving_cards_history`.
Transaction is defined as an activity which change the value in `balance` or `credit_used` columns.
Note: transactions can optionally be calculated for cards/accounts in specific `status`, e.g. `ACTIVE`.

## Spark configuration
As this is intended to be run locally, there is not much of a configuration on Spark side.
Spark is running in local mode, default level of parallelism is set to number of local cores. This kind of configuration should
absolutely NOT be used in production.

## Run locally
Following command will run the application and sleep for 600 seconds (10 minutes) so Spark UI 
on `localhost:4040` can be accessed. Sample dataset `/data` will be used for calculations.
```
cd solutions

python3 -m venv dwh-coding-solution
source dwh-coding-solution/bin/activate
pip3 install -r requirements.txt

cd src
python3 process_transactions.py --wait-for 600
```

## Run Docker container
Following command will first build the docker image and then run it. Port 4040 will be forwarded so Spark UI
can be accessed. Also, source data is mounted to `/src/data` directory.
```
cd solutions
docker build -t process_transactions:v1 .
docker run -it -p 4040:4040 -v {full-path-to-the-repo}/data:/src/data process_transactions:v1
```