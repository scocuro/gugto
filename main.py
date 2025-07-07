#!/usr/bin/env python3
# main.py

import os
import click
from modules.real_estate import main as real_estate_main
from modules.population import main as population_main
from modules.notsold import main as notsold_main

# API 키를 환경변수로부터 가져오기
os.environ.setdefault("MOLIT_STATS_KEY", os.getenv("MOLIT_STATS_KEY", ""))

@click.group()
def cli():
    """데이터 수집 CLI"""
    pass

@cli.command(name="real-estate")
@click.option("--region-name", required=True, help="예: 서울 종로구")
@click.option("--start",       required=True, help="YYYYMM")
@click.option("--end",         required=True, help="YYYYMM")
@click.option("--output", default="real_estate.xlsx")
def real_estate(region_name, start, end, output):
    real_estate_main(region_name, start, end, output)

@cli.command()
@click.option("--start", required=True, help="YYYYMM")
@click.option("--end",   required=True, help="YYYYMM")
@click.option("--output", default="population.xlsx")
def population(start, end, output):
    population_main(start, end, output)

@cli.command()
@click.option("--region-name", required=True, help="예: 서울 종로구")
@click.option("--start",       required=True, help="YYYYMM")
@click.option("--end",         required=True, help="YYYYMM")
@click.option("--output", default="notsold.xlsx")
def notsold(region_name, start, end, output):
    notsold_main(region_name, start, end, output)

if __name__ == "__main__":
    cli()