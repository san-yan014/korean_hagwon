#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job16
#$ -j y

module load python3/3.13.8
python donga_scraping.py 1429021 1523140