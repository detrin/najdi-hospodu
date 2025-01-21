# najdi-hospodu

## Usage

### Local
```
uv venv --python=3.12
source .venv/bin/activate
uv pip install -r requirements.txt
```

```
python3.12 scraping.py --task scraping 
python3.12 scraping.py --task correcting 
# repeat the next step how many times do you want
python3.12 scraping.py --task correcting --results_raw data/results.json
```

https://pid.cz/zastavky-pid/zastavky-v-praze