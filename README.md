# Pub finder
Find the optimal place where to meet with your friends in Prague.

![main](https://github.com/detrin/pub-finder/actions/workflows/test.yml/badge.svg)

Try out the [Web demo](https://huggingface.co/spaces/hermanda/pub-finder), integrated into [Huggingface Spaces 🤗](https://huggingface.co/spaces) using [Gradio](https://github.com/gradio-app/gradio). 

## Usage

### Local
```
uv venv --python=3.12
source .venv/bin/activate
uv pip install -r requirements.txt
python app.py
```
Now you can visit http://0.0.0.0:3000 and enjoy the app.

### Docker
```
docker build -t pub-finder-app .
docker run -p 3000:3000 --name pub-finder pub-finder-app
```
Now you can enjoy the app on http://localhost:3000. 

To remove the image
```
docker rm pdf-summarizer
```


## Development
Prepare the geo data of stops scraped from PID.
```
python3.12 prepare_geo_data.py 
```

For scraping use the following. Repeast until you scrape all the ocmbinations. 
```
python3.12 scraping.py --num_processes 50
```

## Sources
- https://idos.cz/vlakyautobusymhdvse/spojeni/
- https://pid.cz/zastavky-pid/zastavky-v-praze
- https://mapa.pid.cz/?filter=&zoom=12.0&lon=14.4269&lat=50.0874