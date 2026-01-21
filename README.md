# Korean News Analysis: Hagwon Teacher Stigmatization Research

Automated pipeline scraping and analyzing thousands of Korean news articles from major outlets (JoongAng Ilbo, DongA Ilbo, SBS News, Yonhap) studying media portrayal of hagwon (private academy) teachers from 2005-2019.

## Overview

This project supports academic research at Boston University examining occupational stigmatization of hagwon instructors in Korean media. The pipeline processes 7,000+ articles to identify and classify stigmatization patterns across traditional news sources.

## Features

- **Web Scraping Infrastructure**: BeautifulSoup and Selenium-based scrapers handling dynamic content and varying site structures across multiple Korean news portals
- **Translation Pipeline**: Batch processing system converting Korean articles to English while preserving contextual meaning
- **Quote Attribution System**: Pattern recognition and NLP algorithms automatically categorizing speakers (teachers, parents, students, officials) from unstructured text
- **Double-Filter Logic**: Keyword matching requiring both hagwon-related and instructor-specific terms to ensure high relevance
- **16-Code Classification System**: Analyzes stigmatization patterns across five categories:
  - Perpetrator-related content
  - Victim maltreatment
  - Financial compensation issues
  - Working conditions
  - Employment status

## Tech Stack

- **Languages**: Python
- **Web Scraping**: BeautifulSoup, Selenium
- **Data Processing**: pandas, NumPy
- **Database**: PostgreSQL
- **APIs**: Claude Batch API for translation
- **NLP**: Pattern recognition, text classification

## Key Challenges

- **Speaker Attribution**: Korean articles don't always explicitly label quote sources, requiring context-aware pattern recognition
- **Multi-source Data**: Handling different portal structures and anti-scraping measures across news sites
- **Scale & Reliability**: Processing thousands of articles with robust error handling and checkpoint-based recovery
- **Cost Optimization**: Batch processing and caching to manage API translation costs

## Research Context

This pipeline supports research on occupational stigmatization, examining how Korean media portrays hagwon teachers across a 15-year period. The comprehensive classification system enables systematic analysis of media narratives at scale.

## Author

Built by San Yan as part of research with Professor Michel Anteby at Boston University.
