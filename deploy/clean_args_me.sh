export PYTHONPATH="src/:./"
echo $PYTHONPATH
python3 src/runs/run_preprocess_args_me.py
python3 src/runs/run_clean_ids.py
python3 src/runs/run_clean_hash.py