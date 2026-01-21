"""
create excel workbook with configurable charts

usage:
    python create_excel_graphs.py <input.csv>

outputs:
    youtube_graphs.xlsx - excel file with data tables and charts
"""

import pandas as pd
import sys
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
    code = int(code)
    if code in STIGMA:
        return 'Stigma'
    elif code in NON_STIGMA:
        return 'Non-Stigma'
    return None

def load_data(filepath):
    df = pd.read_csv(filepath)
    df['year'] = pd.to_datetime(df['date'], errors='coerce').dt.year
    df = df[(df['code'] != 0) & (df['year'].notna())]
    return df

def create_code_distribution_chart(workbook, worksheet, df, row, col):
    """bar chart of code distribution"""
    counts = Counter(df['code'])
    sorted_counts = dict(sorted(counts.items(), key=lambda x: int(x[0])))
    
    codes = list(sorted_counts.keys())
    values = list(sorted_counts.values())
    total = sum(values)
    percentages = [v/total*100 for v in values]
    
    # write data table
    worksheet.write(row, col, 'Code')
    worksheet.write(row, col+1, 'Count')
    worksheet.write(row, col+2, 'Percentage')
    
    for i, (code, count, pct) in enumerate(zip(codes, values, percentages)):
        worksheet.write(row+1+i, col, int(code))
        worksheet.write(row+1+i, col+1, count)
        worksheet.write(row+1+i, col+2, pct)
    
    # create chart
    chart = workbook.add_chart({'type': 'column'})
    chart.add_series({
        'name': 'Code Distribution',
        'categories': [worksheet.name, row+1, col, row+len(codes), col],
        'values': [worksheet.name, row+1, col+1, row+len(codes), col+1],
        'data_labels': {'value': True, 'percentage': True},
        'fill': {'color': '#E74C3C'},
        'border': {'color': '#2C5F8A'}
    })
    
    chart.set_title({'name': 'Code Distribution Across All Comments (YouTube)'})
    chart.set_x_axis({'name': 'Code'})
    chart.set_y_axis({'name': 'Count'})
    chart.set_size({'width': 720, 'height': 400})
    chart.set_style(11)
    
    worksheet.insert_chart(row, col+5, chart)
    
    return row + len(codes) + 3

def create_code_by_year_chart(workbook, worksheet, df, row, col):
    """grouped bar chart showing codes over years"""
    df_pivot = df.pivot_table(
        index='year',
        columns='code',
        values='text',
        aggfunc='count',
        fill_value=0
    )
    
    years = list(df_pivot.index)
    codes = list(df_pivot.columns)
    
    # write data table
    worksheet.write(row, col, 'Year')
    for i, code in enumerate(codes):
        worksheet.write(row, col+1+i, f'Code {int(code)}')
    
    for i, year in enumerate(years):
        worksheet.write(row+1+i, col, int(year))
        for j, code in enumerate(codes):
            worksheet.write(row+1+i, col+1+j, int(df_pivot.loc[year, code]))
    
    # create chart
    chart = workbook.add_chart({'type': 'column'})
    
    # add series for each code
    for i, code in enumerate(codes[:5]):  # limit to 5 codes for readability
        chart.add_series({
            'name': f'Code {int(code)}',
            'categories': [worksheet.name, row+1, col, row+len(years), col],
            'values': [worksheet.name, row+1, col+1+i, row+len(years), col+1+i],
        })
    
    chart.set_title({'name': 'Code Distribution by Year (YouTube)'})
    chart.set_x_axis({'name': 'Year'})
    chart.set_y_axis({'name': 'Count'})
    chart.set_size({'width': 720, 'height': 400})
    chart.set_style(11)
    
    worksheet.insert_chart(row, col+len(codes)+2, chart)
    
    return row + len(years) + 3

def create_category_proportions_chart(workbook, worksheet, df, row, col):
    """stacked bar chart showing category proportions by year"""
    df_temp = df.copy()
    df_temp['category'] = df_temp['code'].apply(categorize_code)
    
    df_pivot = df_temp.pivot_table(
        index='year',
        columns='category',
        values='text',
        aggfunc='count',
        fill_value=0
    )
    
    years = list(df_pivot.index)
    categories = ['Moral Taint', 'Conduct Taint', 'Contested Role', 'Low Status', 'High Status', 'Neutral']
    categories = [c for c in categories if c in df_pivot.columns]
    
    # write data table
    worksheet.write(row, col, 'Year')
    for i, cat in enumerate(categories):
        worksheet.write(row, col+1+i, cat)
    
    for i, year in enumerate(years):
        worksheet.write(row+1+i, col, int(year))
        for j, cat in enumerate(categories):
            if cat in df_pivot.columns:
                worksheet.write(row+1+i, col+1+j, int(df_pivot.loc[year, cat]))
    
    # create stacked bar chart
    chart = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})
    
    colors = {
        'Moral Taint': '#E74C3C',
        'Conduct Taint': '#E67E22',
        'Contested Role': '#F1C40F',
        'Low Status': '#9B59B6',
        'High Status': '#2ECC71',
        'Neutral': '#3498DB'
    }
    
    for i, cat in enumerate(categories):
        chart.add_series({
            'name': cat,
            'categories': [worksheet.name, row+1, col, row+len(years), col],
            'values': [worksheet.name, row+1, col+1+i, row+len(years), col+1+i],
            'fill': {'color': colors.get(cat, '#95A5A6')}
        })
    
    chart.set_title({'name': 'Category Proportions by Year (YouTube)'})
    chart.set_x_axis({'name': 'Year'})
    chart.set_y_axis({'name': 'Comment Count'})
    chart.set_size({'width': 720, 'height': 400})
    chart.set_style(11)
    
    worksheet.insert_chart(row, col+len(categories)+2, chart)
    
    return row + len(years) + 3

def create_stigma_over_time_chart(workbook, worksheet, df, row, col):
    """grouped bar chart showing stigma vs non-stigma by year"""
    df_temp = df.copy()
    df_temp['stigma'] = df_temp['code'].apply(categorize_stigma)
    
    df_pivot = df_temp.pivot_table(
        index='year',
        columns='stigma',
        values='text',
        aggfunc='count',
        fill_value=0
    )
    
    years = list(df_pivot.index)
    
    # write data table
    worksheet.write(row, col, 'Year')
    worksheet.write(row, col+1, 'Stigma')
    worksheet.write(row, col+2, 'Non-Stigma')
    
    for i, year in enumerate(years):
        worksheet.write(row+1+i, col, int(year))
        worksheet.write(row+1+i, col+1, int(df_pivot.loc[year, 'Stigma']) if 'Stigma' in df_pivot.columns else 0)
        worksheet.write(row+1+i, col+2, int(df_pivot.loc[year, 'Non-Stigma']) if 'Non-Stigma' in df_pivot.columns else 0)
    
    # create grouped bar chart
    chart = workbook.add_chart({'type': 'column'})
    
    chart.add_series({
        'name': 'Stigma',
        'categories': [worksheet.name, row+1, col, row+len(years), col],
        'values': [worksheet.name, row+1, col+1, row+len(years), col+1],
        'fill': {'color': '#E74C3C'}
    })
    
    chart.add_series({
        'name': 'Non-Stigma',
        'categories': [worksheet.name, row+1, col, row+len(years), col],
        'values': [worksheet.name, row+1, col+2, row+len(years), col+2],
        'fill': {'color': '#3498DB'}
    })
    
    chart.set_title({'name': 'Stigma vs Non-Stigma Distribution Over Time (YouTube)'})
    chart.set_x_axis({'name': 'Year'})
    chart.set_y_axis({'name': 'Count'})
    chart.set_size({'width': 720, 'height': 400})
    chart.set_style(11)
    
    worksheet.insert_chart(row, col+5, chart)
    
    return row + len(years) + 3

def create_stigma_percentage_chart(workbook, worksheet, df, row, col):
    """line chart showing stigma percentage over time"""
    df_temp = df.copy()
    df_temp['stigma'] = df_temp['code'].apply(categorize_stigma)
    
    df_pivot = df_temp.pivot_table(
        index='year',
        columns='stigma',
        values='text',
        aggfunc='count',
        fill_value=0
    )
    
    years = list(df_pivot.index)
    
    # calculate percentages
    totals = df_pivot.sum(axis=1)
    stigma_pct = [(df_pivot.loc[y, 'Stigma'] / totals.loc[y] * 100) if 'Stigma' in df_pivot.columns else 0 for y in years]
    
    # write data table
    worksheet.write(row, col, 'Year')
    worksheet.write(row, col+1, 'Stigma %')
    
    for i, (year, pct) in enumerate(zip(years, stigma_pct)):
        worksheet.write(row+1+i, col, int(year))
        worksheet.write(row+1+i, col+1, pct)
    
    # create line chart
    chart = workbook.add_chart({'type': 'line'})
    
    chart.add_series({
        'name': 'Stigma %',
        'categories': [worksheet.name, row+1, col, row+len(years), col],
        'values': [worksheet.name, row+1, col+1, row+len(years), col+1],
        'line': {'color': '#E74C3C', 'width': 2},
        'marker': {'type': 'circle', 'size': 7, 'fill': {'color': '#E74C3C'}}
    })
    
    chart.set_title({'name': 'Stigma Percentage Over Time (YouTube)'})
    chart.set_x_axis({'name': 'Year'})
    chart.set_y_axis({'name': 'Stigma Percentage (%)'})
    chart.set_size({'width': 720, 'height': 400})
    chart.set_style(11)
    
    worksheet.insert_chart(row, col+4, chart)
    
    return row + len(years) + 3

def create_moral_vs_conduct_chart(workbook, worksheet, df, row, col):
    """grouped bar chart showing moral taint vs conduct taint"""
    df_temp = df.copy()
    df_temp['category'] = df_temp['code'].apply(categorize_code)
    df_temp = df_temp[df_temp['category'].isin(['Moral Taint', 'Conduct Taint'])]
    
    df_pivot = df_temp.pivot_table(
        index='year',
        columns='category',
        values='text',
        aggfunc='count',
        fill_value=0
    )
    
    years = list(df_pivot.index)
    
    # write data table
    worksheet.write(row, col, 'Year')
    worksheet.write(row, col+1, 'Moral Taint')
    worksheet.write(row, col+2, 'Conduct Taint')
    
    for i, year in enumerate(years):
        worksheet.write(row+1+i, col, int(year))
        worksheet.write(row+1+i, col+1, int(df_pivot.loc[year, 'Moral Taint']) if 'Moral Taint' in df_pivot.columns else 0)
        worksheet.write(row+1+i, col+2, int(df_pivot.loc[year, 'Conduct Taint']) if 'Conduct Taint' in df_pivot.columns else 0)
    
    # create grouped bar chart
    chart = workbook.add_chart({'type': 'column'})
    
    chart.add_series({
        'name': 'Moral Taint',
        'categories': [worksheet.name, row+1, col, row+len(years), col],
        'values': [worksheet.name, row+1, col+1, row+len(years), col+1],
        'fill': {'color': '#E74C3C'}
    })
    
    chart.add_series({
        'name': 'Conduct Taint',
        'categories': [worksheet.name, row+1, col, row+len(years), col],
        'values': [worksheet.name, row+1, col+2, row+len(years), col+2],
        'fill': {'color': '#E67E22'}
    })
    
    chart.set_title({'name': 'Moral Taint vs Conduct Taint Over Time (YouTube)'})
    chart.set_x_axis({'name': 'Year'})
    chart.set_y_axis({'name': 'Count'})
    chart.set_size({'width': 720, 'height': 400})
    chart.set_style(11)
    
    worksheet.insert_chart(row, col+5, chart)
    
    return row + len(years) + 3

def create_comments_per_year_chart(workbook, worksheet, df, row, col):
    """bar chart showing total comments per year"""
    year_counts = df['year'].value_counts().sort_index()
    years = list(year_counts.index)
    counts = list(year_counts.values)
    
    # write data table
    worksheet.write(row, col, 'Year')
    worksheet.write(row, col+1, 'Total Comments')
    
    for i, (year, count) in enumerate(zip(years, counts)):
        worksheet.write(row+1+i, col, int(year))
        worksheet.write(row+1+i, col+1, count)
    
    # create bar chart
    chart = workbook.add_chart({'type': 'column'})
    
    chart.add_series({
        'name': 'Total Comments',
        'categories': [worksheet.name, row+1, col, row+len(years), col],
        'values': [worksheet.name, row+1, col+1, row+len(years), col+1],
        'fill': {'color': '#34495E'},
        'border': {'color': '#2C3E50'}
    })
    
    chart.set_title({'name': 'Total Comments Per Year (YouTube)'})
    chart.set_x_axis({'name': 'Year'})
    chart.set_y_axis({'name': 'Comment Count'})
    chart.set_size({'width': 720, 'height': 400})
    chart.set_style(11)
    
    worksheet.insert_chart(row, col+4, chart)
    
    return row + len(years) + 3

def create_code5_subcategory_chart(workbook, worksheet, df, row, col):
    """bar chart showing code 5 subcategory distribution"""
    code5_df = df[df['code'] == 5].copy()
    code5_df = code5_df.dropna(subset=['code_5_sub'])
    
    # count subcategories
    sub_counts = {'a': 0, 'b': 0, 'c': 0, 'd': 0}
    for val in code5_df['code_5_sub']:
        for sub in str(val).replace(' ', '').split(','):
            if sub in sub_counts:
                sub_counts[sub] += 1
    
    sub_labels = {
        'a': 'Sexual (students)',
        'b': 'Sexual (non-students)',
        'c': 'Drug-Related',
        'd': 'Violent/Fraud'
    }
    
    # write data table
    worksheet.write(row, col, 'Subcategory')
    worksheet.write(row, col+1, 'Count')
    
    for i, sub in enumerate(['a', 'b', 'c', 'd']):
        worksheet.write(row+1+i, col, sub_labels[sub])
        worksheet.write(row+1+i, col+1, sub_counts[sub])
    
    # create bar chart
    chart = workbook.add_chart({'type': 'column'})
    
    chart.add_series({
        'name': 'Code 5 Subcategories',
        'categories': [worksheet.name, row+1, col, row+4, col],
        'values': [worksheet.name, row+1, col+1, row+4, col+1],
        'data_labels': {'value': True},
        'fill': {'color': '#E74C3C'}
    })
    
    chart.set_title({'name': 'Code 5 (Criminal Conduct) Subcategory Distribution (YouTube)'})
    chart.set_x_axis({'name': 'Subcategory'})
    chart.set_y_axis({'name': 'Count'})
    chart.set_size({'width': 720, 'height': 400})
    chart.set_style(11)
    
    worksheet.insert_chart(row, col+4, chart)
    
    return row + 7

def main():
    if len(sys.argv) < 2:
        print("usage: python create_excel_graphs.py <input.csv>")
        sys.exit(1)
    
    filepath = sys.argv[1]
    output_file = 'youtube_graphs.xlsx'
    
    print(f"loading: {filepath}")
    df = load_data(filepath)
    
    print(f"total comments (excluding code 0): {len(df)}")
    print(f"creating excel workbook with charts...")
    
    # create excel workbook
    workbook = pd.ExcelWriter(output_file, engine='xlsxwriter')
    writer = workbook.book
    
    # sheet 1: code distribution
    worksheet1 = writer.add_worksheet('Code Distribution')
    current_row = 0
    current_row = create_code_distribution_chart(writer, worksheet1, df, current_row, 0)
    
    # sheet 2: code by year
    worksheet2 = writer.add_worksheet('Code by Year')
    current_row = 0
    current_row = create_code_by_year_chart(writer, worksheet2, df, current_row, 0)
    
    # sheet 3: category proportions
    worksheet3 = writer.add_worksheet('Category Proportions')
    current_row = 0
    current_row = create_category_proportions_chart(writer, worksheet3, df, current_row, 0)
    
    # sheet 4: stigma analysis
    worksheet4 = writer.add_worksheet('Stigma Analysis')
    current_row = 0
    current_row = create_stigma_over_time_chart(writer, worksheet4, df, current_row, 0)
    current_row = create_stigma_percentage_chart(writer, worksheet4, df, current_row, 0)
    
    # sheet 5: moral vs conduct
    worksheet5 = writer.add_worksheet('Moral vs Conduct')
    current_row = 0
    current_row = create_moral_vs_conduct_chart(writer, worksheet5, df, current_row, 0)
    
    # sheet 6: comments per year
    worksheet6 = writer.add_worksheet('Comments Per Year')
    current_row = 0
    current_row = create_comments_per_year_chart(writer, worksheet6, df, current_row, 0)
    
    # sheet 7: code 5 subcategories
    worksheet7 = writer.add_worksheet('Code 5 Subcategories')
    current_row = 0
    current_row = create_code5_subcategory_chart(writer, worksheet7, df, current_row, 0)
    
    # sheet 8: raw data
    df.to_excel(workbook, sheet_name='Raw Data', index=False)
    
    workbook.close()
    
    print(f"\n✅ saved: {output_file}")
    print("\nopen the excel file to:")
    print("  - edit chart titles, labels, colors")
    print("  - modify data tables")
    print("  - customize chart styles")
    print("  - add/remove data series")

if __name__ == "__main__":
    main()