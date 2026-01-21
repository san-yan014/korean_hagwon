#!/bin/bash -l

#$ -P koreateach
#$ -N donga_job4
#$ -j y

module load python3/3.13.8
python donga_scraping.py 299593 393712