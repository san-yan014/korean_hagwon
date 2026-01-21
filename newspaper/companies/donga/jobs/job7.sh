#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job7
#$ -j y

module load python3/3.13.8
python donga_scraping.py 581950 676069