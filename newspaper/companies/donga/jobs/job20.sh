#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job20
#$ -j y

module load python3/3.13.8
python donga_scraping.py 1805497 1899633