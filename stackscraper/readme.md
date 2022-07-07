# Scrapy Demo: stackoverflow questions

#### Author: Malcolm
Hello Ryan! This repo contains the code for my demo on writing a Scrapy app in a scrapythonic way using items and pipelines objects.

The website to crawl is [https://stackoverflow.com/questions](https://stackoverflow.com/questions).

The completed project demonstrates a simple big data batch processing pipeline orchestrated by airflow deployed on AWS which parses html using the scrapy framework and ingests new questions daily, processes them in-memory using the spark compute engine, and stores them in a hive data warehouse - to give you a flavor of what we typically do in the bigtech world.

## Background 
Owing to our discussion on what type of object to return for an intial scrape extract function besides json, one of my suggestions were to yield scrapy items objects, which would take full advantage of the scrapy's true capabilities.

Scrapy architecture is designed around the concept of items, which is what gets passed around in a request / response cycle at every stage of the pipeline starting with the crawler spider.

Although the code may look more complicated at first, some of the benefits I brought up in our discussion includes: 
- You can add pre/post processing to each Item field (via ItemLoader) e.g. trimming spaces, and separate this from the main spider logic to keep your code more structured and clean.
- You can take advantage of Item Pipelines class which enables storing scraped items in a database and removing duplicates etc.
- You can examine and debug your scrapy app more easily using built-in UI and stats on items

## Setup
Tested with Python 3.10.4 via virtual environment:
```shell
$ git clone https://github.com/brothermalcolm/scrapy-stackoverflow.git
$ cd scrapy-stackoverflow/
$ python -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

## Run

Run `scrapy crawl questions` with partition key parameter at the project top level.
```shell
$ cd stackscraper
$ scrapy crawl questions -s ds="2022-07-07"
```

Note that spider name is defined in the spider class, e.g., `questions.py`:
```python
class QuestionsSpider(scrapy.Spider):
    name = "questions"
```

## Testing

Run `python -m unittest test_pipeline.py` at the project top level.

Note that daily parameter `ds` is defined in main.py, e.g., '2017-07-06':
```shell
$ python -m unittest test_pipeline.py
```

## Deployment

See deployment.md

## Changelog (versions)

I have built this scrapy project in an agile way with incremental feature additions as follows (italics indicate pending):
- added spider
- added items
- added pipelines
- added models 
- added unittests
*- added airflow*
*- added docker*

### Version 1 (basic spider)

Key Concepts: basic spider setup, project folder structure, saving files as json and html files, using Scrap shell，Following links, etc.

Local outputs (json and html pages) are stored in "data" folder.

For example:

scrapy crawl questions saves a set of html pages to /local_output
scrapy crawl questions -o ./data/questions.json saves the output to a json file



To create the initial project folder, run `scrapy startproject stackscraper` (you can skip this step) and you should get the following project folder structure:

```
stackscraper/
    scrapy.cfg            # deploy configuration file


    stackscraper/             # project's Python module
        __init__.py

        items.py          # project items definition file

        middlewares.py    # project middlewares file

        pipelines.py      # project pipelines file

        settings.py       # project settings file

        spiders/          # a directory where you'll later put your spiders
            __init__.py
```


### Version 2 (items)

The major change is to use Items.

Why use Items?

- clearly specify the structured data to be collected - a central place to look
- leverage pre and post processors for Items via ItemLoaders (you can also define additional custom processors)
- Use item pipelines to save data to databases (Version 3)
- Better code organization - you know where to look for certain processing code

### Version 3 (pipelines)

- Add database support via Spark and use Item pipeline to save items into data warehouse (hive and S3)

Database schema and connection string is defined in `/stackscraper/models.py` file.
Add a pipleline file and enable the pipeline in `/stackscraper/settings.py` by uncommenting the following line (The number 0-1000 specifies the execution order of the pipelines).

```
ITEM_PIPELINES = {
    'tutorial.pipelines.StackscraperPipeline': 300,
}
```

### Version 4 
Airflow scheduler (wip)

### Version 5 
Deployment to AWS (wip)
