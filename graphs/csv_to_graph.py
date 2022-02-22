import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import csv


def pandas_ap():
    """
    https://plotly.com/python/plot-data-from-csv/
    :return:
    """
    df = pd.read_csv('export (2).csv')

    fig = px.line(df, x='MONTH', y='count(DISTINCT body)', title='Count by Month')
    fig.show()


def matplotlib_basic():
    """
    https://www.geeksforgeeks.org/visualize-data-from-csv-file-in-python/
    :return:
    """
    x = []
    y = []

    with open('export (2).csv', 'r') as csvfile:
        plots = list(csv.reader(csvfile, delimiter=','))
        for row in plots[1:]:
            x.append(row[0])
            y.append(int(row[2]))

    plt.bar(x, y, color='g', width=0.72, label="Age")
    plt.xlabel('Month')
    plt.ylabel('Count')
    plt.title('Count by Month')
    plt.legend()
    plt.show()


def matplotlib_pro_chart():
    """
    https://www.machinelearningplus.com/plots/top-50-matplotlib-visualizations-the-master-plots-python/
    :return:
    """
    # Prepare Data
    df_raw = pd.read_csv("https://github.com/selva86/datasets/raw/master/mpg_ggplot2.csv")
    df = df_raw[['cty', 'manufacturer']].groupby('manufacturer').apply(lambda x: x.mean())
    df.sort_values('cty', inplace=True)
    df.reset_index(inplace=True)

    # Draw plot
    fig, ax = plt.subplots(figsize=(16, 10), facecolor='white', dpi=80)
    ax.vlines(x=df.index, ymin=0, ymax=df.cty, color='firebrick', alpha=0.7, linewidth=20)

    # Annotate Text
    for i, cty in enumerate(df.cty):
        ax.text(i, cty + 0.5, round(cty, 1), horizontalalignment='center')

    # Title, Label, Ticks and Ylim
    ax.set_title('Bar Chart for Highway Mileage', fontdict={'size': 22})
    ax.set(ylabel='Miles Per Gallon', ylim=(0, 30))
    plt.xticks(df.index, df.manufacturer.str.upper(), rotation=60, horizontalalignment='right', fontsize=12)

    # Add patches to color the X axis labels
    p1 = patches.Rectangle((.57, -0.005), width=.33, height=.13, alpha=.1, facecolor='red', transform=fig.transFigure)
    p2 = patches.Rectangle((.124, -0.005), width=.446, height=.13, alpha=.1, facecolor='red', transform=fig.transFigure)
    fig.add_artist(p1)
    fig.add_artist(p2)
    plt.show()



if __name__ == "__main__":
    matplotlib_pro_chart()
