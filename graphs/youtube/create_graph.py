"""
youtube comment code analysis graphs

usage:
    python create_youtube_graph.py <input.csv>

input csv columns:
    channel, channel_translated, video_url, text, text_translated, author, date, likes, code, code_5_sub, justification

outputs:
    - yt_code_distribution_bar.png             : bar chart of code frequencies with percentages
    - yt_code_distribution_cumulative.png      : bar chart with cumulative line
    - yt_code_proportions.csv                  : table of code counts and percentages
    - yt_code_by_year/                         : folder with per-code yearly distribution
    - yt_category_proportions_by_year.png      : stacked bar by category (moral taint, conduct taint, etc.)
    - yt_stigma_over_time.png                  : grouped bar of stigma vs non-stigma per year
    - yt_stigma_proportions_by_year.png        : stacked bar of stigma vs non-stigma
    - yt_stigma_percentage_trend.png           : line graph of stigma % over time
    - yt_within_stigma_composition.png         : moral taint vs conduct taint breakdown
    - yt_moral_vs_conduct_over_time.png        : grouped bar of moral vs conduct taint
    - yt_stigma_ratio_over_time.png            : line graph of stigma:non-stigma ratio
    - yt_comments_per_year.png                 : total comment count per year
    - yt_code5_subcategory_distribution.png    : code 5 subcategory breakdown (a, b, c, d)
    - yt_code5_subcategory_over_time.png       : code 5 subcategories by year

categories:
    moral taint:    2
    conduct taint:  1, 4, 5
    contested role: 3, 12
    low status:     6, 7, 9
    high status:    14, 15
    neutral:        8, 10, 11, 13, 16

stigma:     1, 2, 4, 5
non-stigma: 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16

code 5 subcategories:
    a: sexual misconduct (with students)
    b: sexual misconduct (with non-students)
    c: drug-related crimes
    d: violent crimes / financial fraud
"""

import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
from collections import Counter

# category definitions
MORAL_TAINT = [2]
CONDUCT_TAINT = [1, 4, 5]
CONTESTED_ROLE = [3, 12]
LOW_STATUS = [6, 7, 9]
HIGH_STATUS = [14, 15]
NEUTRAL = [8, 10, 11, 13, 16]

# stigma definitions
STIGMA = [1, 2, 4, 5]
NON_STIGMA = [3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

def categorize_code(code):
    # assign category based on code
    code = int(code)
    if code in MORAL_TAINT:
        return 'Moral Taint'
    elif code in CONDUCT_TAINT:
        return 'Conduct Taint'
    elif code in CONTESTED_ROLE:
        return 'Contested Role'
    elif code in LOW_STATUS:
        return 'Low Status'
    elif code in HIGH_STATUS:
        return 'High Status'
    elif code in NEUTRAL:
        return 'Neutral'
    return None

def categorize_stigma(code):
    # assign stigma or non-stigma based on code
    code = int(code)
    if code in STIGMA:
        return 'Stigma'
    elif code in NON_STIGMA:
        return 'Non-Stigma'
    return None

def load_data(filepath):
    # load csv file and extract year from date
    df = pd.read_csv(filepath)
    
    # youtube dates are already in ISO format from scraping (YYYY-MM-DD or similar)
    # just extract the year directly
    df['year'] = pd.to_datetime(df['date'], errors='coerce').dt.year
    
    # filter out code 0 (irrelevant comments) and rows with invalid dates
    df = df[(df['code'] != 0) & (df['year'].notna())]
    
    return df

def get_code_counts(df, code_column='code'):
    # count occurrences of each code
    codes = df[code_column].dropna()
    return Counter(codes)

def plot_bar_chart(counts, title='Code Distribution Across All Comments (YouTube)', output_file='yt_code_distribution_bar.png'):
    # bar chart of code frequencies
    sorted_counts = dict(sorted(counts.items(), key=lambda x: int(x[0])))
    labels = list(sorted_counts.keys())
    values = list(sorted_counts.values())
    total = sum(values)
    
    plt.figure(figsize=(12, 6))
    x = range(len(labels))
    bars = plt.bar(x, values, color='#E74C3C', edgecolor='#2C5F8A', linewidth=0.5, alpha=0.85)
    plt.xlabel('Code', fontsize=11)
    plt.ylabel('Count', fontsize=11)
    plt.title(title, fontsize=14, fontweight='bold')
    plt.xticks(x, labels, ha='center')
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    # add count and percentage labels on bars
    for bar, val in zip(bars, values):
        pct = val / total * 100
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                 f'{val}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=7, color='#333333')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_cumulative_chart(counts, title='Code Distribution Across All Comments with Cumulative Count (YouTube)', output_file='yt_code_distribution_cumulative.png'):
    # bar chart with cumulative line overlay
    sorted_counts = dict(sorted(counts.items(), key=lambda x: int(x[0])))
    labels = list(sorted_counts.keys())
    values = list(sorted_counts.values())
    
    # calculate cumulative values and percentages
    total = sum(values)
    cumulative = []
    cumulative_pct = []
    running = 0
    for v in values:
        running += v
        cumulative.append(running)
        cumulative_pct.append(running / total * 100)
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # bar chart
    x = range(len(labels))
    bars = ax1.bar(x, values, color='#E74C3C', edgecolor='#2C5F8A', linewidth=0.5, alpha=0.85, label='Count')
    ax1.set_xlabel('Code', fontsize=11)
    ax1.set_ylabel('Count', color='#2C5F8A', fontsize=11)
    ax1.tick_params(axis='y', labelcolor='#2C5F8A')
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, ha='center')
    
    # cleaner look
    ax1.spines['top'].set_visible(False)
    ax1.grid(axis='y', linestyle='--', alpha=0.3)
    
    # cumulative line on secondary axis
    ax2 = ax1.twinx()
    ax2.plot(x, cumulative_pct, color='#3498DB', marker='o', markersize=5, linewidth=2, label='Cumulative %')
    ax2.fill_between(x, cumulative_pct, color='#3498DB', alpha=0.08)
    ax2.set_ylabel('Cumulative %', color='#3498DB', fontsize=11)
    ax2.set_ylim(0, 105)
    ax2.tick_params(axis='y', labelcolor='#3498DB')
    ax2.spines['top'].set_visible(False)
    
    # add percentage labels on cumulative line
    for i, pct in enumerate(cumulative_pct):
        ax2.text(i, pct + 2, f'{pct:.1f}%', ha='center', va='bottom', fontsize=7, color='#E85A5A')
    
    plt.title(title, fontsize=14, fontweight='bold')
    fig.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_code_by_year(df, code_column='code', year_column='year', output_dir='yt_code_by_year'):
    # create separate graph for each code showing count over years
    os.makedirs(output_dir, exist_ok=True)
    
    codes = df[code_column].dropna().unique()
    codes = sorted(codes, key=lambda x: int(x))
    
    for code in codes:
        code_df = df[df[code_column] == code]
        year_counts = code_df[year_column].value_counts().sort_index()
        
        years = list(year_counts.index)
        counts = list(year_counts.values)
        
        plt.figure(figsize=(10, 5))
        x = range(len(years))
        bars = plt.bar(x, counts, color='#E74C3C', edgecolor='#2C5F8A', linewidth=0.5, alpha=0.85)
        plt.xlabel('Year', fontsize=11)
        plt.ylabel('Count', fontsize=11)
        plt.title(f'Code {code} Distribution by Year (YouTube)', fontsize=14, fontweight='bold')
        plt.xticks(x, [str(int(y)) for y in years], ha='center')
        
        # cleaner look
        plt.gca().spines['top'].set_visible(False)
        plt.gca().spines['right'].set_visible(False)
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        # add count labels
        for bar, val in zip(bars, counts):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                     str(val), ha='center', va='bottom', fontsize=8, color='#333333')
        
        plt.tight_layout()
        output_file = os.path.join(output_dir, f'code_{code}_by_year.png')
        plt.savefig(output_file, dpi=300)
        plt.close()
        print(f"saved: {output_file}")

def plot_proportion_table(counts, output_file='yt_code_proportions.csv'):
    # save proportions as csv table
    total = sum(counts.values())
    data = []
    for code, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
        proportion = count / total * 100
        data.append({'code': code, 'count': count, 'proportion': f'{proportion:.2f}%'})
    
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    print(f"saved: {output_file}")

def plot_category_proportions_by_year(df, code_column='code', year_column='year', output_file='yt_category_proportions_by_year.png'):
    # stacked bar graph showing counts with proportions labeled
    df = df.copy()
    df['category'] = df[code_column].apply(categorize_code)
    df = df.dropna(subset=['category', year_column])
    
    # group by year and category
    grouped = df.groupby([year_column, 'category']).size().unstack(fill_value=0)
    years = sorted(grouped.index)
    
    # calculate proportions for labels
    totals = grouped.sum(axis=1)
    proportions = grouped.div(totals, axis=0) * 100
    
    plt.figure(figsize=(12, 6))
    
    x = range(len(years))
    width = 0.6
    
    colors = {'Moral Taint': '#E74C3C', 'Conduct Taint': '#E67E22', 'Contested Role': '#F1C40F', 'Low Status': '#9B59B6', 'High Status': '#2ECC71', 'Neutral': '#3498DB'}
    
    # stacked bars using counts
    bottom = [0] * len(years)
    for category in ['Moral Taint', 'Conduct Taint', 'Contested Role', 'Low Status', 'Neutral', 'High Status']:
        if category in grouped.columns:
            counts = [grouped.loc[y, category] if y in grouped.index else 0 for y in years]
            pcts = [proportions.loc[y, category] if y in proportions.index else 0 for y in years]
            plt.bar(x, counts, width, bottom=bottom, label=category, color=colors[category], edgecolor='white', linewidth=0.5, alpha=0.85)
            
            # add percentage labels
            for i, (cnt, pct, b) in enumerate(zip(counts, pcts, bottom)):
                if pct > 5:
                    plt.text(i, b + cnt/2, f'{pct:.1f}%', ha='center', va='center', fontsize=8, color='white', fontweight='bold')
            
            bottom = [b + c for b, c in zip(bottom, counts)]
    
    # add total count labels on top
    for i, total in enumerate([totals.loc[y] for y in years]):
        plt.text(i, total + 2, str(total), ha='center', va='bottom', fontsize=8, color='#333333', fontweight='bold')
    
    plt.xlabel('Year', fontsize=11)
    plt.ylabel('Comment Count', fontsize=11)
    plt.title('Category Proportions by Year (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(x, [str(int(y)) for y in years])
    plt.legend(loc='upper right')
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_stigma_over_time(df, code_column='code', year_column='year', output_file='yt_stigma_over_time.png'):
    # grouped bar graph showing stigma vs non-stigma count per year
    df = df.copy()
    df['stigma'] = df[code_column].apply(categorize_stigma)
    df = df.dropna(subset=['stigma', year_column])
    
    # group by year and stigma
    grouped = df.groupby([year_column, 'stigma']).size().unstack(fill_value=0)
    years = sorted(grouped.index)
    
    plt.figure(figsize=(12, 6))
    
    x = range(len(years))
    width = 0.35
    
    colors = {'Stigma': '#E74C3C', 'Non-Stigma': '#3498DB'}
    
    # grouped bars
    stigma_vals = [grouped.loc[y, 'Stigma'] if 'Stigma' in grouped.columns else 0 for y in years]
    non_stigma_vals = [grouped.loc[y, 'Non-Stigma'] if 'Non-Stigma' in grouped.columns else 0 for y in years]
    
    bars1 = plt.bar([i - width/2 for i in x], stigma_vals, width, label='Stigma', color=colors['Stigma'], edgecolor='white', linewidth=0.5, alpha=0.85)
    bars2 = plt.bar([i + width/2 for i in x], non_stigma_vals, width, label='Non-Stigma', color=colors['Non-Stigma'], edgecolor='white', linewidth=0.5, alpha=0.85)
    
    # add count labels
    for bar in bars1:
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5, str(int(bar.get_height())), ha='center', va='bottom', fontsize=7, color='#333333')
    for bar in bars2:
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5, str(int(bar.get_height())), ha='center', va='bottom', fontsize=7, color='#333333')
    
    plt.xlabel('Year', fontsize=11)
    plt.ylabel('Count', fontsize=11)
    plt.title('Stigma vs Non-Stigma Distribution Over Time (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(x, [str(int(y)) for y in years])
    plt.legend()
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_stigma_proportions_by_year(df, code_column='code', year_column='year', output_file='yt_stigma_proportions_by_year.png'):
    # stacked bar graph showing stigma vs non-stigma counts with proportions
    df = df.copy()
    df['stigma'] = df[code_column].apply(categorize_stigma)
    df = df.dropna(subset=['stigma', year_column])
    
    # group by year and stigma
    grouped = df.groupby([year_column, 'stigma']).size().unstack(fill_value=0)
    years = sorted(grouped.index)
    
    # calculate proportions for labels
    totals = grouped.sum(axis=1)
    proportions = grouped.div(totals, axis=0) * 100
    
    plt.figure(figsize=(12, 6))
    
    x = range(len(years))
    width = 0.6
    
    colors = {'Stigma': '#E74C3C', 'Non-Stigma': '#3498DB'}
    
    # stacked bars using counts
    bottom = [0] * len(years)
    for stigma in ['Stigma', 'Non-Stigma']:
        if stigma in grouped.columns:
            counts = [grouped.loc[y, stigma] if y in grouped.index else 0 for y in years]
            pcts = [proportions.loc[y, stigma] if y in proportions.index else 0 for y in years]
            plt.bar(x, counts, width, bottom=bottom, label=stigma, color=colors[stigma], edgecolor='white', linewidth=0.5, alpha=0.85)
            
            # add percentage labels
            for i, (cnt, pct, b) in enumerate(zip(counts, pcts, bottom)):
                if pct > 5:
                    plt.text(i, b + cnt/2, f'{pct:.1f}%', ha='center', va='center', fontsize=8, color='white', fontweight='bold')
            
            bottom = [b + c for b, c in zip(bottom, counts)]
    
    # add total count labels on top
    for i, total in enumerate([totals.loc[y] for y in years]):
        plt.text(i, total + 5, str(total), ha='center', va='bottom', fontsize=8, color='#333333', fontweight='bold')
    
    plt.xlabel('Year', fontsize=11)
    plt.ylabel('Comment Count', fontsize=11)
    plt.title('Stigma vs Non-Stigma Proportions by Year (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(x, [str(int(y)) for y in years])
    plt.legend(loc='upper right')
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_stigma_percentage_trend(df, code_column='code', year_column='year', output_file='yt_stigma_percentage_trend.png'):
    # line graph showing stigma percentage over time
    df = df.copy()
    df['stigma'] = df[code_column].apply(categorize_stigma)
    df = df.dropna(subset=['stigma', year_column])
    
    # group by year and stigma
    grouped = df.groupby([year_column, 'stigma']).size().unstack(fill_value=0)
    years = sorted(grouped.index)
    
    # calculate stigma percentage
    totals = grouped.sum(axis=1)
    stigma_pct = [(grouped.loc[y, 'Stigma'] / totals.loc[y] * 100) if 'Stigma' in grouped.columns else 0 for y in years]
    
    plt.figure(figsize=(12, 6))
    
    plt.plot(years, stigma_pct, marker='o', linewidth=2, color='#E74C3C', label='Stigma %')
    plt.fill_between(years, stigma_pct, color='#E74C3C', alpha=0.1)
    
    # add percentage labels
    for y, pct in zip(years, stigma_pct):
        plt.text(y, pct + 1, f'{pct:.1f}%', ha='center', va='bottom', fontsize=8, color='#E74C3C')
    
    plt.xlabel('Year', fontsize=11)
    plt.ylabel('Stigma Percentage (%)', fontsize=11)
    plt.title('Stigma Percentage Over Time (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(years, [str(int(y)) for y in years])
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_within_stigma_composition(df, code_column='code', year_column='year', output_file='yt_within_stigma_composition.png'):
    # stacked bar showing moral taint vs conduct taint as % of stigma comments
    df = df.copy()
    df['category'] = df[code_column].apply(categorize_code)
    # filter to only stigma categories
    df = df[df['category'].isin(['Moral Taint', 'Conduct Taint'])]
    df = df.dropna(subset=['category', year_column])
    
    # group by year and category
    grouped = df.groupby([year_column, 'category']).size().unstack(fill_value=0)
    years = sorted(grouped.index)
    
    # calculate proportions
    totals = grouped.sum(axis=1)
    proportions = grouped.div(totals, axis=0) * 100
    
    plt.figure(figsize=(12, 6))
    
    x = range(len(years))
    width = 0.6
    
    colors = {'Moral Taint': '#E74C3C', 'Conduct Taint': '#E67E22'}
    
    # stacked bars
    bottom = [0] * len(years)
    for category in ['Moral Taint', 'Conduct Taint']:
        if category in grouped.columns:
            counts = [grouped.loc[y, category] if y in grouped.index else 0 for y in years]
            pcts = [proportions.loc[y, category] if y in proportions.index else 0 for y in years]
            plt.bar(x, counts, width, bottom=bottom, label=category, color=colors[category], edgecolor='white', linewidth=0.5, alpha=0.85)
            
            # add percentage labels
            for i, (cnt, pct, b) in enumerate(zip(counts, pcts, bottom)):
                if pct > 5:
                    plt.text(i, b + cnt/2, f'{pct:.1f}%', ha='center', va='center', fontsize=8, color='white', fontweight='bold')
            
            bottom = [b + c for b, c in zip(bottom, counts)]
    
    # add total count labels on top
    for i, total in enumerate([totals.loc[y] for y in years]):
        plt.text(i, total + 1, str(total), ha='center', va='bottom', fontsize=8, color='#333333', fontweight='bold')
    
    plt.xlabel('Year', fontsize=11)
    plt.ylabel('Stigma Comment Count', fontsize=11)
    plt.title('Within-Stigma Composition: Moral Taint vs Conduct Taint (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(x, [str(int(y)) for y in years])
    plt.legend(loc='upper right')
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_moral_vs_conduct_over_time(df, code_column='code', year_column='year', output_file='yt_moral_vs_conduct_over_time.png'):
    # grouped bar graph showing moral taint vs conduct taint count per year
    df = df.copy()
    df['category'] = df[code_column].apply(categorize_code)
    df = df[df['category'].isin(['Moral Taint', 'Conduct Taint'])]
    df = df.dropna(subset=['category', year_column])
    
    # group by year and category
    grouped = df.groupby([year_column, 'category']).size().unstack(fill_value=0)
    years = sorted(grouped.index)
    
    plt.figure(figsize=(12, 6))
    
    x = range(len(years))
    width = 0.35
    
    colors = {'Moral Taint': '#E74C3C', 'Conduct Taint': '#E67E22'}
    
    # grouped bars
    moral_vals = [grouped.loc[y, 'Moral Taint'] if 'Moral Taint' in grouped.columns else 0 for y in years]
    conduct_vals = [grouped.loc[y, 'Conduct Taint'] if 'Conduct Taint' in grouped.columns else 0 for y in years]
    
    bars1 = plt.bar([i - width/2 for i in x], moral_vals, width, label='Moral Taint', color=colors['Moral Taint'], edgecolor='white', linewidth=0.5, alpha=0.85)
    bars2 = plt.bar([i + width/2 for i in x], conduct_vals, width, label='Conduct Taint', color=colors['Conduct Taint'], edgecolor='white', linewidth=0.5, alpha=0.85)
    
    # add count labels
    for bar in bars1:
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, str(int(bar.get_height())), ha='center', va='bottom', fontsize=7, color='#333333')
    for bar in bars2:
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, str(int(bar.get_height())), ha='center', va='bottom', fontsize=7, color='#333333')
    
    plt.xlabel('Year', fontsize=11)
    plt.ylabel('Count', fontsize=11)
    plt.title('Moral Taint vs Conduct Taint Over Time (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(x, [str(int(y)) for y in years])
    plt.legend()
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_stigma_ratio_over_time(df, code_column='code', year_column='year', output_file='yt_stigma_ratio_over_time.png'):
    # line graph showing stigma:non-stigma ratio over time
    df = df.copy()
    df['stigma'] = df[code_column].apply(categorize_stigma)
    df = df.dropna(subset=['stigma', year_column])
    
    # group by year and stigma
    grouped = df.groupby([year_column, 'stigma']).size().unstack(fill_value=0)
    years = sorted(grouped.index)
    
    # calculate ratio
    ratios = []
    for y in years:
        stigma = grouped.loc[y, 'Stigma'] if 'Stigma' in grouped.columns else 0
        non_stigma = grouped.loc[y, 'Non-Stigma'] if 'Non-Stigma' in grouped.columns else 1
        ratios.append(stigma / non_stigma if non_stigma > 0 else 0)
    
    plt.figure(figsize=(12, 6))
    
    plt.plot(years, ratios, marker='o', linewidth=2, color='#9B59B6', label='Stigma:Non-Stigma Ratio')
    plt.fill_between(years, ratios, color='#9B59B6', alpha=0.1)
    
    # add ratio labels
    for y, ratio in zip(years, ratios):
        plt.text(y, ratio + 0.02, f'{ratio:.2f}', ha='center', va='bottom', fontsize=8, color='#9B59B6')
    
    # add reference line at 1.0
    plt.axhline(y=1.0, color='#333333', linestyle='--', linewidth=1, alpha=0.5, label='1:1 ratio')
    
    plt.xlabel('Year', fontsize=11)
    plt.ylabel('Ratio (Stigma / Non-Stigma)', fontsize=11)
    plt.title('Stigma to Non-Stigma Ratio Over Time (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(years, [str(int(y)) for y in years])
    plt.legend()
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_comments_per_year(df, year_column='year', output_file='yt_comments_per_year.png'):
    # bar graph showing total comment count per year
    year_counts = df[year_column].value_counts().sort_index()
    years = list(year_counts.index)
    counts = list(year_counts.values)
    
    plt.figure(figsize=(12, 6))
    
    x = range(len(years))
    bars = plt.bar(x, counts, color='#34495E', edgecolor='#2C3E50', linewidth=0.5, alpha=0.85)
    
    # add count labels
    for bar, val in zip(bars, counts):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5, str(val), ha='center', va='bottom', fontsize=8, color='#333333')
    
    plt.xlabel('Year', fontsize=11)
    plt.ylabel('Comment Count', fontsize=11)
    plt.title('Total Comments Per Year (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(x, [str(int(y)) for y in years])
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_code5_subcategory(df, output_file='yt_code5_subcategory_distribution.png'):
    # bar graph showing code 5 subcategory distribution
    code5_df = df[df['code'] == 5].copy()
    code5_df = code5_df.dropna(subset=['code_5_sub'])
    
    # split multiple subcategories and count individually
    sub_counts = {'a': 0, 'b': 0, 'c': 0, 'd': 0}
    for val in code5_df['code_5_sub']:
        for sub in str(val).replace(' ', '').split(','):
            if sub in sub_counts:
                sub_counts[sub] += 1
    
    # subcategory labels
    sub_labels = {
        'a': 'Sexual Misconduct\n(with students)',
        'b': 'Sexual Misconduct\n(with non-students)',
        'c': 'Drug-Related\nCrimes',
        'd': 'Violent Crimes /\nFinancial Fraud'
    }
    
    labels = [sub_labels[s] for s in ['a', 'b', 'c', 'd']]
    values = [sub_counts[s] for s in ['a', 'b', 'c', 'd']]
    total = sum(values)
    
    plt.figure(figsize=(10, 6))
    
    x = range(len(labels))
    colors = ['#E74C3C', '#E67E22', '#9B59B6', '#3498DB']
    bars = plt.bar(x, values, color=colors, edgecolor='white', linewidth=0.5, alpha=0.85)
    
    # add count and percentage labels
    for bar, val in zip(bars, values):
        pct = val / total * 100 if total > 0 else 0
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                 f'{val}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=9, color='#333333')
    
    plt.xlabel('Subcategory', fontsize=11)
    plt.ylabel('Count', fontsize=11)
    plt.title('Code 5 (Criminal Conduct) Subcategory Distribution (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(x, labels, ha='center')
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def plot_code5_subcategory_over_time(df, year_column='year', output_file='yt_code5_subcategory_over_time.png'):
    # stacked bar graph showing code 5 subcategories by year
    code5_df = df[df['code'] == 5].copy()
    code5_df = code5_df.dropna(subset=['code_5_sub', year_column])
    
    # get all years
    years = sorted(code5_df[year_column].unique())
    
    # count each subcategory per year (splitting multiple values)
    data = {sub: [] for sub in ['a', 'b', 'c', 'd']}
    for year in years:
        year_df = code5_df[code5_df[year_column] == year]
        year_counts = {'a': 0, 'b': 0, 'c': 0, 'd': 0}
        for val in year_df['code_5_sub']:
            for sub in str(val).replace(' ', '').split(','):
                if sub in year_counts:
                    year_counts[sub] += 1
        for sub in ['a', 'b', 'c', 'd']:
            data[sub].append(year_counts[sub])
    
    # calculate totals and proportions
    totals = [sum(data[sub][i] for sub in ['a', 'b', 'c', 'd']) for i in range(len(years))]
    
    # subcategory labels
    sub_labels = {
        'a': 'Sexual (students)',
        'b': 'Sexual (non-students)',
        'c': 'Drug-Related',
        'd': 'Violent/Fraud'
    }
    
    plt.figure(figsize=(12, 6))
    
    x = range(len(years))
    width = 0.6
    
    colors = {'a': '#E74C3C', 'b': '#E67E22', 'c': '#9B59B6', 'd': '#3498DB'}
    
    # stacked bars
    bottom = [0] * len(years)
    for sub in ['a', 'b', 'c', 'd']:
        counts = data[sub]
        pcts = [c / t * 100 if t > 0 else 0 for c, t in zip(counts, totals)]
        plt.bar(x, counts, width, bottom=bottom, label=sub_labels[sub], color=colors[sub], edgecolor='white', linewidth=0.5, alpha=0.85)
        
        # add percentage labels
        for i, (cnt, pct, b) in enumerate(zip(counts, pcts, bottom)):
            if pct > 10:
                plt.text(i, b + cnt/2, f'{pct:.0f}%', ha='center', va='center', fontsize=7, color='white', fontweight='bold')
        
        bottom = [b + c for b, c in zip(bottom, counts)]
    
    # add total count labels on top
    for i, total in enumerate(totals):
        if total > 0:
            plt.text(i, total + 0.5, str(int(total)), ha='center', va='bottom', fontsize=8, color='#333333', fontweight='bold')
    
    plt.xlabel('Year', fontsize=11)
    plt.ylabel('Count', fontsize=11)
    plt.title('Code 5 (Criminal Conduct) Subcategories by Year (YouTube)', fontsize=14, fontweight='bold')
    plt.xticks(x, [str(int(y)) for y in years])
    plt.legend(loc='upper right')
    
    # cleaner look
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"saved: {output_file}")

def main():
    if len(sys.argv) < 2:
        print("usage: python create_youtube_graph.py <input.csv>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    code_column = 'code'
    year_column = 'year'
    
    print(f"loading: {filepath}")
    df = load_data(filepath)
    
    print(f"counting codes from column: {code_column}")
    counts = get_code_counts(df, code_column)
    
    print(f"total comments (excluding code 0): {len(df)}")
    print(f"unique codes: {len(counts)}")
    
    # generate all graphs
    plot_bar_chart(counts)
    plot_cumulative_chart(counts)
    plot_proportion_table(counts)
    plot_code_by_year(df, code_column, year_column)
    plot_category_proportions_by_year(df, code_column, year_column)
    plot_stigma_over_time(df, code_column, year_column)
    plot_stigma_proportions_by_year(df, code_column, year_column)
    plot_stigma_percentage_trend(df, code_column, year_column)
    plot_within_stigma_composition(df, code_column, year_column)
    plot_moral_vs_conduct_over_time(df, code_column, year_column)
    plot_stigma_ratio_over_time(df, code_column, year_column)
    plot_comments_per_year(df, year_column)
    plot_code5_subcategory(df)
    plot_code5_subcategory_over_time(df, year_column)
    
    print("done!")

if __name__ == "__main__":
    main()