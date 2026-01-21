#!/bin/bash -l

#$ -P koreateach
#$ -N sbs_scraping
#$ -j y

module load python3/3.13.8
python sbs_scraping.py 