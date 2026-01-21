#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job17
#$ -j y

module load python3/3.13.8
python donga_scraping.py 1523140 1617259