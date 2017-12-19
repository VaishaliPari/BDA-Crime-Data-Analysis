#Staten Island Each year count
#!/usr/bin/python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import csv

#define pie chart
def make_autopct(values):
    def my_autopct(pct):
        total = sum(values)
        val = int(round(pct*total/100.0))
        return '{p:.2f}%\n({v:d})'.format(p=pct,v=val)
    return my_autopct
#read input file
crimes = []
borough = []
with open('total_borough_yearly_STATEN.out','r') as file:
    plots = csv.reader(file, delimiter='\t')
    for row in plots:
        crimes.append(int(row[0]))
        borough.append(row[1])
#plot the pie chart
fig, ax = plt.subplots(figsize=(15,15))
ax.pie(crimes, labels=borough, autopct=make_autopct(crimes),
        shadow=True, startangle=90)
ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
plt.title('Crime Amount At Each Borough')
plt.legend(loc=5)
# plt.show()
plt.savefig('total_borough_yearly_STATEN.png')
