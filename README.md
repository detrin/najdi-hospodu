# Pub finder
Find the optimal place where to meet with your friends in Prague.

Try out the [Web demo](https://huggingface.co/spaces/hermanda/pub-finder), integrated into [Huggingface Spaces 🤗](https://huggingface.co/spaces) using [Gradio](https://github.com/gradio-app/gradio). 

## Usage

### Local
```
uv venv --python=3.12
source .venv/bin/activate
uv pip install -r requirements.txt
```

```
# repeat the next step how many times do you want
python3.12 scraping.py --num_processes 50
```

## Sources
- https://idos.cz/vlakyautobusymhdvse/spojeni/
- https://pid.cz/zastavky-pid/zastavky-v-praze