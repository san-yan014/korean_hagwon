#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job14
#$ -j y

module load python3/3.13.8
python donga_scraping.py 1240783 1334902