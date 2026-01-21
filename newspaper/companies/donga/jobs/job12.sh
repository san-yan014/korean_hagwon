#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job12
#$ -j y

module load python3/3.13.8
python donga_scraping.py 1052545 1146664