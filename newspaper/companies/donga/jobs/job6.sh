#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job6
#$ -j y

module load python3/3.13.8
python donga_scraping.py 487831 581950