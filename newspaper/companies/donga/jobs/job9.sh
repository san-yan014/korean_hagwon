#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job9
#$ -j y

module load python3/3.13.8
python donga_scraping.py 770188 864307