#!/bin/bash -l

#$ -P koreateach
#$ -N youtube_scraping
#$ -l h_rt=24:00:00

module load python3/3.13.8

pip install --user youtube-comment-downloader

python youtube_scraping.py --api-key AIzaSyDfkzqybna2xOe5L81RRHTRNYBOnTlw62k --skip-dates --resume