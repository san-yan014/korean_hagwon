#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job1
#$ -j y

module load python3/3.13.8
python yna_scraping.py 