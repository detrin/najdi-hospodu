# najdi-hospodu
Find the optimal place where to meet with your friends in Prague.

Try out the [Web demo](https://huggingface.co/spaces/hermanda/najdi-hospodu), integrated into [Huggingface Spaces 🤗](https://huggingface.co/spaces) using [Gradio](https://github.com/gradio-app/gradio). 

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

## Sources
- https://idos.cz/vlakyautobusymhdvse/spojeni/
- https://pid.cz/zastavky-pid/zastavky-v-praze