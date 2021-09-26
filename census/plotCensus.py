# -*- coding: utf-8 -*-
#%% plot census data -- above
import matplotlib.pyplot as plt
import pandas as pd
data = pd.read_csv("data/data-ratio.csv")
popdata = data[['Year','Population',"# Private Vehicle", "# Bus", "# Taxi","# Bus Passenger"]]
#popdata.plot(kind="line",x='Year')

from matplotlib.ticker import FuncFormatter
ax = popdata.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Year",grid=True,markeredgecolor="black",ms=10)
ax.set_ylabel("Relative Difference",fontsize=20)
ax.legend(loc=0,fontsize=20)#,labels=["Light Rain","No Rain","Heavy Rain"])
ax.set_xlabel("Year",fontsize=20)
#ax.set_ylim((0.24,0.55))
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=3, mode="expand",fontsize=10 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/pop-above.pdf", bbox_inches='tight')

#%% plot censuss data -- under
import matplotlib.pyplot as plt
import pandas as pd
data = pd.read_csv("data/data-ratio.csv")
popdata = data[['Year','Population',"# Subway Passenger","# Subway Lines"]]
#popdata.plot(kind="line",x='Year')

from matplotlib.ticker import FuncFormatter
ax = popdata.plot(kind="line",fontsize =20,style=["bs-","yo-","rs-"], x="Year",grid=True,markeredgecolor="black",ms=10)
ax.set_ylabel("Relative Difference",fontsize=20)
ax.legend(loc=0,fontsize=20)#,labels=["Light Rain","No Rain","Heavy Rain"])
ax.set_xlabel("Year",fontsize=20)
#ax.set_ylim((0.24,0.55))
plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
       ncol=2, mode="expand",fontsize=10 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/pop-under.pdf", bbox_inches='tight')

#%% combine two together
import matplotlib.pyplot as plt
import pandas as pd
data = pd.read_csv("data/data-ratio.csv")
popdata = data[['Year','Population',"# Private Vehicle","# Bus Passenger","# Subway Passenger"]]
popdata.columns = ['Year','Population','Personal Cars','Bus Passenger','Subway Passenger']
#popdata.plot(kind="line",x='Year')
popdata = popdata[['Year','Subway Passenger','Personal Cars','Bus Passenger','Population']]
from matplotlib.ticker import FuncFormatter
ax = popdata.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Year",grid=True,markeredgecolor="black",ms=10)
ax.set_ylabel("Relative Difference",fontsize=20)
ax.legend(loc=0,fontsize=15)#,labels=["Light Rain","No Rain","Heavy Rain"])
ax.set_xlabel("Year",fontsize=20)
#ax.set_ylim((0.24,0.55))
#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=0,
#       ncol=2, mode="expand",fontsize=15 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/pop-all.pdf", bbox_inches='tight') 
#%% combine two together
import matplotlib.pyplot as plt
import pandas as pd
data = pd.read_csv("data/data-ratio.csv")
popdata = data[["Year","Length of Roads","Length of Road Per Capita"]]
#popdata.plot(kind="line",x='Year')

from matplotlib.ticker import FuncFormatter
ax = popdata.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Year",grid=True,markeredgecolor="black",ms=10)
ax.set_ylabel("Relative Difference",fontsize=20)
ax.set_xlabel("Year",fontsize=20)
ax.legend(loc=0,fontsize=14)#,labels=["Light Rain","No Rain","Heavy Rain"])

#ax.set_ylim((0.24,0.55))
#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
#       ncol=1, mode="expand",fontsize=13 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/roads.pdf", bbox_inches='tight')
#%% length of subway line
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
data = pd.read_csv("data/roads.csv")
popdata = data[["Year","Length of Roads","Length of Road Per Capita","Length of Subway Line per Passenger"]]
#popdata = popdata[["Year","Length of Subway Line per Passenger","Length of Road Per Capita","Length of Roads"]]
#popdata.plot(kind="line",x='Year')
popdata.columns = ["Year","Length of Roads","Length of Roads Per Capita","Length of Subway per Passenger"]
from matplotlib.ticker import FuncFormatter
ax = popdata.plot(kind="line",fontsize =20,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Year",grid=True,markeredgecolor="black",ms=10)
ax.set_ylabel("Relative Difference",fontsize=20)
ax.set_xlabel("Year",fontsize=20)
ax.legend(loc=0,fontsize=15)#,labels=["Light Rain","No Rain","Heavy Rain"])

plt.xticks(np.arange(2013,2017,1))
plt.ylim(-0.3,0.05)
#ax.set_ylim((0.24,0.55))
#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
#       ncol=1, mode="expand",fontsize=13 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: '{:.0%}'.format(y)))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/roads.pdf", bbox_inches='tight')
#%% census - above 
import matplotlib.pyplot as plt
import pandas as pd
data = pd.read_csv("data/data-ratio.csv")
popdata = data[['Year','Population',"# Private Vehicle","# Bus Passenger","# Subway Passenger"]]
popdata.columns = ['Year','Pop','Cars','Bus Passenger','Subway']
#popdata.plot(kind="line",x='Year')
popdata = popdata[['Year','Subway','Cars','Pop']]
from matplotlib.ticker import FuncFormatter
ax = popdata.plot(kind="line",fontsize =23,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Year",grid=True,markeredgecolor="black",ms=15,lw=3)
ax.set_ylabel("Relative Increase (%)",fontsize=28)
ax.legend(loc=0,fontsize=25)#,labels=["Light Rain","No Rain","Heavy Rain"])
ax.set_xlabel("",fontsize=0)
#ax.set_ylim((0.24,0.55))
#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=0,
#       ncol=2, mode="expand",fontsize=15 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: int(y*100)))
plt.yticks(np.arange(0,2.1,0.5))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/pop-all.pdf", bbox_inches='tight') 

#%% length of subway line
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
data = pd.read_csv("data/roads.csv")
popdata = data[["Year","Length of Road Per Capita","Length of Subway Line per Passenger"]]
#popdata = popdata[["Year","Length of Subway Line per Passenger","Length of Road Per Capita","Length of Roads"]]
#popdata.plot(kind="line",x='Year')
popdata.columns = ["Year","Subway","Roads"]
from matplotlib.ticker import FuncFormatter
ax = popdata.plot(kind="line",fontsize =23,style=["bs-","rd:","go-","^-.",">--","rs-"], x="Year",grid=True,markeredgecolor="black",ms=15,lw=3)
ax.set_ylabel("Relative Increase (%)",fontsize=28)
ax.set_xlabel("",fontsize=0)
ax.legend(loc=0,fontsize=25)#,labels=["Light Rain","No Rain","Heavy Rain"])

plt.xticks(np.arange(2013,2017,1))
plt.ylim(-0.3,0.05)
#ax.set_ylim((0.24,0.55))
#plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
#       ncol=1, mode="expand",fontsize=13 ,borderaxespad=0.)
plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda y, _: int(y*100)))
plt.yticks(np.arange(-0.4,0.05,0.1))
#plt.legend(loc=0,fontsize=15,mode='expand',borderaxespad=0,ncol=3,bbox_to_anchor=(0,1.14,1,.106))
plt.savefig("plots/roads.pdf", bbox_inches='tight')
#%%