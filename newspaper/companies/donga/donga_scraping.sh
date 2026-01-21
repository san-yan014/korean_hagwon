#!/bin/bash -l

#$ -P koreateach
#$ -N donga_scraping

#$ -j y

module load python3/3.13.8

python donga_scraping.py 