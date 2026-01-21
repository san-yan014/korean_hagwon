#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job15
#$ -j y

module load python3/3.13.8
python donga_scraping.py 1334902 1429021