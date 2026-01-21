#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job11
#$ -j y

module load python3/3.13.8
python donga_scraping.py 958426 1052545