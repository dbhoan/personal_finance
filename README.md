This project provides tools for an automated system for personal finance.

merge.py:
  This tool loads transaction data in csv format from various sources/formats (bank account activities, credit card statements), 
  clean and check for data gaps, add end-of-day balance for each account (taking US/CAD conversion rate into account) and merge 
  them together in a unified format so that it can be easily analyzed and visualized.
 
tools.py:
  A set of tools to download daily stock price and FX rates from online sources. 
  
plotbalance.ipynb:
  Jupyter notebook to plot balance based on the output of merge.py
