#years vs number of records
#!/usr/bin/python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import numpy as np

#get input file
week, crimes = np.loadtxt("each_weekday_count.out", delimiter='\t', unpack=True)
weeks = ('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')

#function for graph rects
def auto_label(rects):
	for rect in rects:
		height = rect.get_height()
		ax.text(rect.get_x()+rect.get_width()/2.0, height, '%d'%int(height), ha='center', va='bottom')

# formatting the coordinators
fig, ax = plt.subplots(figsize=(10,5))
x_pos = np.arange(len(weeks))
rect = ax.bar(x_pos, crimes, color='blue')
ax.set_xticks(x_pos)
ax.set_xticklabels(weeks)
#set labels
ax.set_ylabel('Crime Count')
ax.set_title('Crime Count for each Weekday')
auto_label(rect)
plt.plot(crimes, 'r')
# plt.show()
plt.savefig('each_weekday_count.png')
