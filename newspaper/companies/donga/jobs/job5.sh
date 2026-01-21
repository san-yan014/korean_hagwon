#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job5
#$ -j y

module load python3/3.13.8
python donga_scraping.py 393712 487831