#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job2
#$ -j y

module load python3/3.13.8
python donga_scraping.py 111355 205474