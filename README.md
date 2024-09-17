# eBay Scraper Project

## Overview
This project is a highly efficient and robust web scraping script using Scrapy. It is designed to scrape approximately 3,900 eBay links provided in an `input.json` file, extract the HTML body of each link, and save it in a specified format. The script includes features such as asynchrony, proxy management, logging, and retry logic.

## Prerequisites
Before you begin, ensure you have met the following requirements:
- You have installed Python 3.11.
- You have installed `pip`, the Python package installer.

### 2. Create a Virtual Environment
- python3 -m venv env

### 3. Activate Virtual Enviroment
- . env/bin/activate for linux or . env/Scripts/activate for windows

### 4. go to project directory
- cd ebay_scraper

## Installation
- pip install -r requirements.txt

### run script 
- scrapy crawl ebay