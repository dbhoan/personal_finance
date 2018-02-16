# Automated system for personal finance

merge.py:
  Loads transaction data in csv format from various sources/formats (bank account activities, credit card statements), 
  cleans and checks for data gaps, adds end-of-day balance for each account (taking US/CAD conversion rate into account) and merges 
  all data together to an output file in a unified format so that it can be easily analyzed and visualized.

plotbalance.ipynb:
  Jupyter notebook to plot balance based on the output of merge.py

tools.py:
  A set of tools to download daily stock price and FX rates from online sources. 
  

